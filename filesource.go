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
	"context"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var currentOpenFiles int64

type FileSource struct {
	*shutter.Shutter

	// blocksStore is where we access the blocks archives.
	blocksStore dstore.Store
	// blockReaderFactory creates a new `BlockReader` from an `io.Reader` instance
	blockReaderFactory BlockReaderFactory

	startBlockNum uint64
	stopBlockNum  uint64
	bundleSize    uint64

	preprocFunc PreprocessFunc
	// gates incoming blocks based on Gator type BEFORE pre-processing
	gator Gator

	handler Handler

	// retryDelay determines the time between attempts to retry the
	// download of blocks archives (most of the time, waiting for the
	// blocks archive to be written by some other process in semi
	// real-time)
	retryDelay              time.Duration
	preprocessorThreadCount int

	// fileStream is a chan of blocks coming from blocks archives, ordered
	// and parallel processed
	fileStream                chan *incomingBlocksFile
	highestFileProcessedBlock BlockRef
	blockIndexProvider        BlockIndexProvider

	// these blocks will be included even if the filter does not want them.
	// If we are on a chain that skips block numbers, the NEXT block will be sent.
	whitelistedBlocks map[uint64]bool

	// if no blocks match filter in a big range, we will still send "some" blocks to help mark progress
	// every time we have not matched any blocks for that duration
	timeBetweenProgressBlocks time.Duration

	logger *zap.Logger
}

type FileSourceOption = func(s *FileSource)

func FileSourceWithConcurrentPreprocess(preprocFunc PreprocessFunc, threadCount int) FileSourceOption {
	return func(s *FileSource) {
		s.preprocessorThreadCount = threadCount
		s.preprocFunc = preprocFunc
	}
}

func FileSourceWithWhitelistedBlocks(nums ...uint64) FileSourceOption {
	return func(s *FileSource) {
		if s.whitelistedBlocks == nil {
			s.whitelistedBlocks = make(map[uint64]bool)
		}
		for _, num := range nums {
			s.whitelistedBlocks[num] = true
		}
	}
}

func FileSourceWithRetryDelay(delay time.Duration) FileSourceOption {
	return func(s *FileSource) {
		s.retryDelay = delay
	}
}
func FileSourceWithStopBlock(stopBlock uint64) FileSourceOption {
	return func(s *FileSource) {
		s.stopBlockNum = stopBlock
	}
}

func FileSourceWithBundleSize(bundleSize uint64) FileSourceOption {
	return func(s *FileSource) {
		s.bundleSize = bundleSize
	}
}

func FileSourceWithBlockIndexProvider(prov BlockIndexProvider) FileSourceOption {
	return func(s *FileSource) {
		s.blockIndexProvider = prov
	}
}

type FileSourceFactory struct {
	mergedBlocksStore dstore.Store
	forkedBlocksStore dstore.Store
	logger            *zap.Logger
	options           []FileSourceOption
}

func NewFileSourceFactory(
	mergedBlocksStore dstore.Store,
	forkedBlocksStore dstore.Store,
	logger *zap.Logger,
	options ...FileSourceOption,
) *FileSourceFactory {
	return &FileSourceFactory{
		mergedBlocksStore: mergedBlocksStore,
		forkedBlocksStore: forkedBlocksStore,
		logger:            logger,
		options:           options,
	}
}

func (g *FileSourceFactory) SourceFromBlockNum(start uint64, h Handler) Source {
	return NewFileSource(
		g.mergedBlocksStore,
		start,
		h,
		g.logger,
		g.options...,
	)
}

func (g *FileSourceFactory) SourceFromCursor(cursor *Cursor, h Handler) Source {
	return NewFileSourceFromCursor(
		g.mergedBlocksStore,
		g.forkedBlocksStore,
		cursor,
		h,
		g.logger,
		g.options...,
	)
}

func NewFileSourceFromCursor(
	mergedBlocksStore dstore.Store,
	forkedBlocksStore dstore.Store,
	cursor *Cursor,
	h Handler,
	logger *zap.Logger,
	options ...FileSourceOption,
) *FileSource {

	wrappedHandler := newCursorResolverHandler(forkedBlocksStore, cursor, h, logger)

	// first block after cursor's block/lib will be sent even if they don't match filter
	// cursor's block/lib also need to match
	tweakedOptions := append(options, FileSourceWithWhitelistedBlocks(
		cursor.LIB.Num(),
		cursor.LIB.Num()+1,
		cursor.Block.Num(),
		cursor.Block.Num()+1,
	))

	return NewFileSource(
		mergedBlocksStore,
		cursor.LIB.Num(),
		wrappedHandler,
		logger,
		tweakedOptions...)

}

func fileSourceBundleSizeFromOptions(options []FileSourceOption) uint64 {
	dummyFileSource := NewFileSource(nil, 0, nil, nil, options...)
	for _, opt := range options {
		opt(dummyFileSource)
	}
	return dummyFileSource.bundleSize
}

func NewFileSource(
	blocksStore dstore.Store,
	startBlockNum uint64,
	h Handler,
	logger *zap.Logger,
	options ...FileSourceOption,

) *FileSource {
	blockReaderFactory := GetBlockReaderFactory

	s := &FileSource{
		startBlockNum:             startBlockNum,
		bundleSize:                100,
		blocksStore:               blocksStore,
		blockReaderFactory:        blockReaderFactory,
		fileStream:                make(chan *incomingBlocksFile, 1),
		Shutter:                   shutter.New(),
		retryDelay:                4 * time.Second,
		timeBetweenProgressBlocks: 30 * time.Second,
		handler:                   h,
		logger:                    logger,
	}

	for _, option := range options {
		option(s)
	}

	return s
}

func (s *FileSource) Run() {
	s.Shutdown(s.run())
}

func (s *FileSource) checkExists(baseBlockNum uint64) (bool, string, error) {
	baseFilename := fmt.Sprintf("%010d", baseBlockNum)
	exists, err := s.blocksStore.FileExists(context.Background(), baseFilename)
	return exists, baseFilename, err
}

func (s *FileSource) run() (err error) {

	go s.launchSink()

	baseBlockNum := lowBoundary(s.startBlockNum, s.bundleSize)
	var delay time.Duration
	for {
		select {
		case <-s.Terminating():
			s.logger.Info("blocks archive streaming was asked to stop")
			return s.Err()
		case <-time.After(delay):
		}

		var filteredBlocks []uint64
		if s.blockIndexProvider != nil {
			nextBase, matching, noMoreIndex := s.lookupBlockIndex(baseBlockNum)
			if noMoreIndex {
				s.blockIndexProvider = nil

				exists, _, _ := s.checkExists(nextBase)
				if !exists && nextBase > baseBlockNum {
					matching = nil
					nextBase -= s.bundleSize
					s.logger.Debug("index pushing us farther than the last bundle, reading previous one entirely", zap.Uint64("next_base", nextBase))
				} else {
					if nextExists, _, _ := s.checkExists(nextBase + s.bundleSize); !nextExists {
						matching = nil
						s.logger.Debug("index pushing us to the last bundle, reading it entirely", zap.Uint64("next_base", nextBase))
					}
				}
			}

			filteredBlocks = matching
			baseBlockNum = nextBase
		}

		exists, baseFilename, err := s.checkExists(baseBlockNum)
		if err != nil {
			return fmt.Errorf("reading file existence: %w", err)
		}

		if !exists {
			s.logger.Info("reading from blocks store: file does not (yet?) exist, retrying in", zap.String("filename", s.blocksStore.ObjectPath(baseFilename)), zap.String("base_filename", baseFilename), zap.Any("retry_delay", s.retryDelay))
			delay = s.retryDelay
			continue
		}
		delay = 0 * time.Second

		// container that is sent to s.fileStream
		newIncomingFile := newIncomingBlocksFile(baseBlockNum, baseFilename, filteredBlocks)

		select {
		case <-s.Terminating():
			return s.Err()
		case s.fileStream <- newIncomingFile:
			zlog.Debug("new incoming file", zap.String("filename", newIncomingFile.filename))
		}

		go func() {
			s.logger.Debug("launching processing of file", zap.String("base_filename", baseFilename))
			if err := s.streamIncomingFile(newIncomingFile, s.blocksStore); err != nil {
				s.Shutdown(fmt.Errorf("processing of file %q failed: %w", baseFilename, err))
			}
		}()

		baseBlockNum += s.bundleSize
		if s.stopBlockNum != 0 && baseBlockNum > s.stopBlockNum {
			close(s.fileStream)
			<-s.Terminating()
			return nil
		}
	}
}

func unique(s []uint64) (result []uint64) {
	inResult := make(map[uint64]bool)
	for _, i := range s {
		if _, ok := inResult[i]; !ok {
			inResult[i] = true
			result = append(result, i)
		}
	}
	return result
}

func (s *FileSource) tweakRangeIndexResults(baseBlock uint64, inBlocks []uint64) []uint64 {
	var addBlocks []uint64
	for wl := range s.whitelistedBlocks {
		if wl < baseBlock {
			delete(s.whitelistedBlocks, wl)
			continue
		}
		if wl < baseBlock+s.bundleSize {
			addBlocks = append(addBlocks, wl)
			delete(s.whitelistedBlocks, wl)
			continue
		}
	}
	if baseBlock <= s.startBlockNum && baseBlock+s.bundleSize > s.startBlockNum {
		addBlocks = append(addBlocks, s.startBlockNum)
	}

	if s.stopBlockNum != 0 && baseBlock <= s.stopBlockNum && baseBlock+s.bundleSize > s.stopBlockNum {
		addBlocks = append(addBlocks, s.stopBlockNum)
	}

	if addBlocks == nil {
		return inBlocks
	}

	allBlocks := append(inBlocks, addBlocks...)
	sort.Slice(allBlocks, func(i, j int) bool { return allBlocks[i] < allBlocks[j] })

	var uniqueBoundedBlocks []uint64

	seen := make(map[uint64]bool)
	for _, blk := range allBlocks {
		if blk < s.startBlockNum {
			continue
		}
		if s.stopBlockNum != 0 && blk > s.stopBlockNum {
			continue
		}
		if _, ok := seen[blk]; !ok {
			seen[blk] = true
			uniqueBoundedBlocks = append(uniqueBoundedBlocks, blk)
		}
	}

	return uniqueBoundedBlocks
}

func (s *FileSource) lookupBlockIndex(in uint64) (baseBlock uint64, outBlocks []uint64, noMoreIndex bool) {
	if s.stopBlockNum != 0 && in > s.stopBlockNum {
		return in, nil, true
	}

	begin := time.Now()
	baseBlock = in
	for {
		filteredBlocks, err := s.blockIndexProvider.BlocksInRange(baseBlock, s.bundleSize)
		if err != nil {
			s.logger.Debug("blocks_in_range returns error, deactivating",
				zap.Uint64("base_block", baseBlock),
				zap.Error(err),
			)
			return baseBlock, nil, true
		}

		outBlocks := s.tweakRangeIndexResults(baseBlock, filteredBlocks)
		if outBlocks == nil {
			if time.Since(begin) >= s.timeBetweenProgressBlocks {
				return baseBlock, []uint64{baseBlock}, false
			}
			baseBlock += s.bundleSize
			continue
		}

		return baseBlock, outBlocks, false
	}
}

func (s *FileSource) streamReader(blockReader BlockReader, prevLastBlockRead BlockRef, incomingBlockFile *incomingBlocksFile) (lastBlockRead BlockRef, err error) {
	var previousLastBlockPassed bool
	if prevLastBlockRead == nil {
		previousLastBlockPassed = true
	}

	done := make(chan interface{})
	preprocessed := make(chan chan *PreprocessedBlock, s.preprocessorThreadCount)

	go func() {
		defer close(done)
		defer close(incomingBlockFile.blocks)

		for {
			select {
			case <-s.Terminating():
				return
			case ppChan, ok := <-preprocessed:
				if !ok {
					return
				}
				select {
				case <-s.Terminating():
					return
				case preprocessBlock := <-ppChan:
					select {
					case <-s.Terminating():
						return
					case incomingBlockFile.blocks <- preprocessBlock:
						lastBlockRead = preprocessBlock.Block.AsRef()
					}
				}
			}
		}
	}()

	for {
		if s.IsTerminating() {
			return
		}

		var blk *Block
		blk, err = blockReader.Read()
		if err != nil && err != io.EOF {
			close(preprocessed)
			return lastBlockRead, err
		}

		if err == io.EOF && (blk == nil || blk.Num() == 0) {
			close(preprocessed)
			break
		}

		BlocksReadFileSource.Inc()
		BytesReadFileSource.AddInt(blk.Payload.Size())

		blockNum := blk.Num()
		if blockNum < s.startBlockNum {
			continue
		}
		if blockNum < incomingBlockFile.baseNum {
			s.logger.Debug("skipping invalid block in file", zap.Uint64("file_base_num", incomingBlockFile.baseNum), zap.Uint64("block_num", blockNum))
			continue
		}

		if !incomingBlockFile.PassesFilter(blockNum) {
			continue
		}

		if !previousLastBlockPassed {
			s.logger.Debug("skipping because this is not the first attempt and we have not seen prevLastBlockRead yet", zap.Stringer("block", blk), zap.Stringer("prev_last_block_read", prevLastBlockRead))
			if prevLastBlockRead.ID() == blk.ID() {
				previousLastBlockPassed = true
			}
			continue
		}

		if s.gator != nil && !s.gator.Pass(blk) {
			s.logger.Debug("gator not passed dropping block")
			continue
		}

		out := make(chan *PreprocessedBlock, 1)

		select {
		case <-s.Terminating():
			return
		case preprocessed <- out:
		}
		go s.preprocess(blk, out)
	}

	<-done
	return lastBlockRead, nil
}

func (s *FileSource) preprocess(block *Block, out chan *PreprocessedBlock) {
	var obj interface{}
	var err error
	if s.preprocFunc != nil {
		obj, err = s.preprocFunc(block)
		if err != nil {
			s.Shutdown(fmt.Errorf("preprocess block: %s: %w", block, err))
			return
		}
	}
	obj = &wrappedObject{
		obj: obj,
		cursor: &Cursor{
			Step:      StepNewIrreversible,
			Block:     block.AsRef(),
			LIB:       block.AsRef(),
			HeadBlock: block.AsRef(),
		}}

	zlog.Debug("block pre processed", zap.Stringer("block_ref", block))
	BlocksSentFileSource.Inc()
	BytesSentFileSource.AddInt(block.Payload.Size())
	select {
	case <-s.Terminating():
		return
	case out <- &PreprocessedBlock{Block: block, Obj: obj}:
	}
}

func (s *FileSource) streamIncomingFile(newIncomingFile *incomingBlocksFile, blocksStore dstore.Store) error {
	atomic.AddInt64(&currentOpenFiles, 1)
	s.logger.Debug("open files", zap.Int64("count", atomic.LoadInt64(&currentOpenFiles)), zap.String("filename", newIncomingFile.filename))
	defer atomic.AddInt64(&currentOpenFiles, -1)

	var skipBlocksBefore BlockRef

	reader, err := blocksStore.OpenObject(context.Background(), newIncomingFile.filename)
	if err != nil {
		return fmt.Errorf("fetching %s from block store: %w", newIncomingFile.filename, err)
	}
	defer reader.Close()

	blockReader, err := s.blockReaderFactory.New(reader)
	if err != nil {
		return fmt.Errorf("unable to create block reader: %w", err)
	}

	if _, err := s.streamReader(blockReader, skipBlocksBefore, newIncomingFile); err != nil {
		return fmt.Errorf("error processing incoming file: %w", err)
	}
	return nil
}

func (s *FileSource) launchSink() {
	for {
		select {
		case <-s.Terminating():
			zlog.Debug("terminating by launch sink")
			return
		case incomingFile, ok := <-s.fileStream:
			if !ok {
				s.Shutdown(nil)
				return
			}
			s.logger.Debug("feeding from incoming file", zap.String("filename", incomingFile.filename))

			for preBlock := range incomingFile.blocks {
				if s.IsTerminating() {
					return
				}

				if err := s.handler.ProcessBlock(preBlock.Block, preBlock.Obj); err != nil {
					s.Shutdown(fmt.Errorf("process block failed: %w", err))
					return
				}
				if s.highestFileProcessedBlock != nil && preBlock.Num() > s.highestFileProcessedBlock.Num() {
					s.highestFileProcessedBlock = preBlock
				}
			}
		}
	}
}

func (s *FileSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}
