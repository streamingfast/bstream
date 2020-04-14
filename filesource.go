// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/dfuse-io/logging"

	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

var currentOpenFiles int64
var fileSourceLogger *zap.Logger

func init() {
	logging.Register("github.com/dfuse-io/bstream/filesource.go", &fileSourceLogger)
}

type FileSource struct {
	Name string

	*shutter.Shutter

	// blocksStore is where we access the blocks archives.
	blocksStore dstore.Store

	// blockReaderFactory creates a new `BlockReader` from an `io.Reader` instance
	blockReaderFactory BlockReaderFactory

	startBlockNum uint64
	preprocFunc   PreprocessFunc
	// gates incoming blocks based on Gator type BEFORE pre-processing
	gator Gator

	// fileStream is a chan of blocks coming from blocks archives, ordered
	// and parallelly processed
	fileStream chan *incomingBlocksFile

	handler Handler
	// retryDelay determines the time between attempts to retry the
	// download of blocks archives (most of the time, waiting for the
	// blocks archive to be written by some other process in semi
	// real-time)
	retryDelay time.Duration

	notFoundCallback func(uint64)
}

type FileSourceOption = func(s *FileSource)

func FileSourceWithTimeThresholdGator(threshold time.Duration) FileSourceOption {
	return func(s *FileSource) {
		zlog.Info("setting time gator",
			zap.Duration("threshold", threshold),
		)
		s.gator = NewTimeThresholdGator(threshold)
	}
}

// NewFileSource will pipe potentially stream you 99 blocks before the given `startBlockNum`.
func NewFileSource(
	blocksStore dstore.Store,
	startBlockNum uint64,
	parallelDownloads int,
	preprocFunc PreprocessFunc,
	h Handler,
	options ...FileSourceOption,
) *FileSource {
	blockReaderFactory := GetBlockReaderFactory

	s := &FileSource{
		startBlockNum:      startBlockNum,
		blocksStore:        blocksStore,
		blockReaderFactory: blockReaderFactory,
		fileStream:         make(chan *incomingBlocksFile, parallelDownloads),
		Shutter:            shutter.New(),
		preprocFunc:        preprocFunc,
		retryDelay:         4 * time.Second,
		handler:            h,
	}
	for _, option := range options {
		option(s)
	}
	return s
}

// SetNotFoundCallback sets a callback function to be triggered when
// a blocks file is not found. Useful for joining with unmerged blocks
func (s *FileSource) SetNotFoundCallback(f func(missingBlockNum uint64)) {
	s.notFoundCallback = f
}

func (s *FileSource) Run() {
	s.Shutdown(s.run())
}

func (s *FileSource) run() error {
	const filesBlocksIncrement = 100 /// HARD-CODED CONFIG HERE!

	go s.launchSink()

	currentIndex := s.startBlockNum
	var delay time.Duration
	for {
		time.Sleep(delay)

		if s.IsTerminating() {
			fileSourceLogger.Info("blocks archive streaming was asked to stop")
			return nil
		}

		baseBlockNum := currentIndex - (currentIndex % filesBlocksIncrement)
		fileSourceLogger.Debug("file stream looking for", zap.Uint64("base_block_num", baseBlockNum))

		baseFilename := fmt.Sprintf("%010d", baseBlockNum)
		exists, err := s.blocksStore.FileExists(baseFilename)
		if err != nil {
			return fmt.Errorf("reading file existence: %s", err)
		}

		if !exists {
			fileSourceLogger.Info("reading from blocks store: file does not (yet?) exist, retrying in", zap.String("filename", s.blocksStore.ObjectPath(baseFilename)), zap.String("base_filename", baseFilename), zap.Any("retry_delay", s.retryDelay))
			delay = s.retryDelay

			if s.notFoundCallback != nil {
				fileSourceLogger.Info("file not found callback set, calling it", zap.Uint64("base_block_num", baseBlockNum))
				mergerBaseBlockNum := baseBlockNum
				if mergerBaseBlockNum < GetProtocolFirstBlock {
					mergerBaseBlockNum =  GetProtocolFirstBlock
				}
				s.notFoundCallback(mergerBaseBlockNum)
			}
			continue
		}
		delay = 0 * time.Second

		newIncomingFile := &incomingBlocksFile{
			filename: baseFilename,
			blocks:   make(chan *PreprocessedBlock, 200), // We target 100 blocks per file, would be surprising we hit 200.
		}

		fileSourceLogger.Debug("downloading archive file", zap.String("filename", newIncomingFile.filename))
		select {
		case <-s.Terminating():
			return s.Err()
		case s.fileStream <- newIncomingFile:
		}

		go func() {
			fileSourceLogger.Debug("launching processing of file", zap.String("base_filename", baseFilename))
			if err := s.streamIncomingFile(newIncomingFile); err != nil {
				s.Shutdown(fmt.Errorf("processing of file %q failed: %s", baseFilename, err))
			}
		}()

		currentIndex += filesBlocksIncrement
	}
}

func (s *FileSource) streamIncomingFile(newIncomingFile *incomingBlocksFile) error {
	defer func() {
		close(newIncomingFile.blocks)
	}()

	atomic.AddInt64(&currentOpenFiles, 1)
	fileSourceLogger.Debug("open files", zap.Int64("count", atomic.LoadInt64(&currentOpenFiles)), zap.String("filename", newIncomingFile.filename))
	defer atomic.AddInt64(&currentOpenFiles, -1)

	// FIXME: Eventually, RETRY for this given file.. and continue to write to `newIncomingFile`.
	reader, err := s.blocksStore.OpenObject(newIncomingFile.filename)
	if err != nil {
		return fmt.Errorf("fetching %s from blockStore: %s", newIncomingFile.filename, err)
	}
	defer reader.Close()

	blockReader, err := s.blockReaderFactory.New(reader)
	if err != nil {
		return fmt.Errorf("unable to create block reader: %s", err)
	}

	for {
		if s.IsTerminating() {
			fileSourceLogger.Info("shutting down incoming batch file download", zap.String("filename", newIncomingFile.filename))
			return nil
		}

		blk, err := blockReader.Read()
		if err != nil && err != io.EOF {
			return fmt.Errorf("block reader failed: %s", err)
		}

		// EOF can happen with valid data, so let's skip if no block defined
		if err == io.EOF && (blk == nil || blk.Num() == 0) {
			return nil
		}

		blockNum := blk.Num()
		if blockNum < s.startBlockNum {
			continue
		}

		if s.gator != nil && !s.gator.Pass(blk) {
			fileSourceLogger.Debug("gator not passed dropping block")
			continue
		}

		var obj interface{}
		if s.preprocFunc != nil {
			obj, err = s.preprocFunc(blk)
			if err != nil {
				return fmt.Errorf("pre-process block failed: %s", err)
			}
		}

		newIncomingFile.blocks <- &PreprocessedBlock{Block: blk, Obj: obj}
		if err == io.EOF {
			return nil
		}
	}
}

func (s *FileSource) launchSink() {
	for {
		select {
		case <-s.Terminating():
			return
		case incomingFile := <-s.fileStream:
			fileSourceLogger.Debug("feeding from incoming file", zap.String("filename", incomingFile.filename))

			for preBlock := range incomingFile.blocks {
				if s.IsTerminating() {
					return
				}

				if err := s.handler.ProcessBlock(preBlock.Block, preBlock.Obj); err != nil {
					s.Shutdown(fmt.Errorf("process block failed: %s", err))
					return
				}
			}
		}
	}

}

func (s *FileSource) SetLogger(logger *zap.Logger) {
	fileSourceLogger = logger
}
