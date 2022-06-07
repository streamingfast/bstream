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
	"sync/atomic"
	"time"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

func (s *FileSource) runOneBlockFile() error {

	go s.launchSinkOneBlock()

	//todo : check if gate setup in one block mode
	if s.gator != nil {
		panic("one block file source do not support gator")
	}

	ctx := context.Background()
	currentBlock := s.startBlockNum
	var delay time.Duration
	for {
		time.Sleep(delay)

		if s.IsTerminating() {
			s.logger.Info("blocks archive streaming was asked to stop")
			return nil
		}

		s.logger.Debug("one block file stream looking for", zap.Uint64("base_block_num", currentBlock))

		store := s.blocksStore
		filePrefix := fmt.Sprintf("%010d", currentBlock)

		var files []string
		err := store.Walk(ctx, filePrefix, func(filename string) (err error) {
			files = append(files, filename)
			return nil
		})
		if err != nil {

			return fmt.Errorf("walking prefix %s: %w", filePrefix, err)
		}

		exists := len(files) > 0

		if !exists {
			s.logger.Info("reading from blocks store: file does not (yet?) exist, retrying in", zap.String("filename_prefix", store.ObjectPath(filePrefix)), zap.Any("retry_delay", s.retryDelay))
			delay = s.retryDelay
			if s.notFoundCallback != nil {
				if err := s.notFoundCallback(currentBlock, s.highestFileProcessedBlock, s.handler, s.logger); err != nil {
					s.logger.Debug("not found callback return an error, shutting down source")
					return fmt.Errorf("not found callback returned an err: %w", err)
				}
			}
			continue
		}
		delay = 0 * time.Second

		newIncomingOneBlockFiles := &incomingOneBlockFiles{
			filenames: files,
			blocks:    make(chan *PreprocessedBlock, 2),
		}

		s.logger.Debug("downloading one block file", zap.Strings("filename", newIncomingOneBlockFiles.filenames))
		select {
		case <-s.Terminating():
			return s.Err()
		case s.oneBlockFileStream <- newIncomingOneBlockFiles:
		}

		go func() {
			s.logger.Debug("launching processing of file", zap.String("base_filename", filePrefix))
			if err := s.streamIncomingOneBlockFile(newIncomingOneBlockFiles, store); err != nil {
				s.Shutdown(fmt.Errorf("processing of file %q failed: %w", filePrefix, err))
			}
		}()

		currentBlock++
	}
}

func (s *FileSource) readOneBlock(fileName string, blocksStore dstore.Store) (*PreprocessedBlock, error) {
	atomic.AddInt64(&currentOpenFiles, 1)
	s.logger.Debug("opening one block file", zap.String("filename", fileName), zap.Int64("currently_open_file_count", currentOpenFiles))
	defer atomic.AddInt64(&currentOpenFiles, -1)

	// FIXME: Eventually, RETRY for this given file.. and continue to write to `newIncomingFile`.
	reader, err := blocksStore.OpenObject(context.Background(), fileName)
	if err != nil {
		return nil, fmt.Errorf("fetching %s from block store: %w", fileName, err)
	}
	defer reader.Close()

	blockReader, err := s.blockReaderFactory.New(reader)
	if err != nil {
		return nil, fmt.Errorf("unable to create block reader: %w", err)
	}

	if s.IsTerminating() {
		s.logger.Info("shutting down incoming batch file download", zap.String("filename", fileName))
		return nil, nil
	}

	blk, err := blockReader.Read()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("block reader failed: %w", err)
	}

	var obj interface{}
	if s.preprocFunc != nil {
		obj, err = s.preprocFunc(blk)
		if err != nil {
			return nil, fmt.Errorf("pre-process block failed: %w", err)
		}
	}

	return &PreprocessedBlock{Block: blk, Obj: obj}, nil

}

func (s *FileSource) streamIncomingOneBlockFile(incomingFiles *incomingOneBlockFiles, blocksStore dstore.Store) error {
	defer func() {
		close(incomingFiles.blocks)
	}()

	for _, fileName := range incomingFiles.filenames {
		preprocessedBlock, err := s.readOneBlock(fileName, blocksStore)
		if err != nil {
			return fmt.Errorf("reading one block file: %w", err)
		}
		incomingFiles.blocks <- preprocessedBlock
		if err == io.EOF {
			return nil
		}
	}

	return nil
}

func (s *FileSource) launchSinkOneBlock() {
	for {
		select {
		case <-s.Terminating():
			return
		case incomingFile := <-s.oneBlockFileStream:
			s.logger.Debug("feeding from incoming file", zap.Strings("filename", incomingFile.filenames))

			for preBlock := range incomingFile.blocks {
				if s.IsTerminating() {
					return
				}

				if err := s.handler.ProcessBlock(preBlock.Block, preBlock.Obj); err != nil {
					s.Shutdown(fmt.Errorf("process block failed: %w", err))
					return
				}
			}
		}
	}
}
