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
	"errors"
	"fmt"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

func NewIndexedFileSource(
	chain *ChainConfig,
	handler Handler,
	preprocFunc PreprocessFunc,
	indexManager *BlockIndexesManager,
	blockStores []dstore.Store,
	unindexedSourceFactory SourceFromNumFactory,
	unindexedHandlerFactory func(h Handler, lib BlockRef) Handler,
	logger *zap.Logger,
	steps StepType,
	cursor *Cursor,
) *IndexedFileSource {
	sendNew := StepNew&steps != 0
	sendIrr := StepIrreversible&steps != 0

	return &IndexedFileSource{
		Shutter:                 shutter.New(),
		chainConfig:             chain,
		logger:                  logger,
		cursor:                  cursor,
		handler:                 handler,
		preprocFunc:             preprocFunc,
		blockIndexManager:       indexManager,
		blockStores:             blockStores,
		sendNew:                 sendNew,
		sendIrr:                 sendIrr,
		unindexedSourceFactory:  unindexedSourceFactory,
		unindexedHandlerFactory: unindexedHandlerFactory,
	}

}

type IndexedFileSource struct {
	*shutter.Shutter

	chainConfig *ChainConfig

	logger        *zap.Logger
	handler       Handler
	lastProcessed BlockRef
	cursor        *Cursor

	blockIndexManager *BlockIndexesManager
	blockStores       []dstore.Store
	sendNew           bool
	sendIrr           bool

	skipCount               uint64
	unindexedSourceFactory  SourceFromNumFactory
	unindexedHandlerFactory func(h Handler, lib BlockRef) Handler

	preprocFunc PreprocessFunc
}

func (s *IndexedFileSource) Run() {
	s.Shutdown(s.run())
}

var SkipToNextRange = errors.New("skip to next range")
var NoMoreIndex = errors.New("no more index")
var SkipThisBlock = errors.New("skip this block")

func (s *IndexedFileSource) SetLogger(l *zap.Logger) {
	s.logger = l
}

func (s *IndexedFileSource) run() error {
	if s.cursor != nil && s.cursor.Step != StepNew && s.cursor.Step != StepIrreversible {
		return fmt.Errorf("invalid cursor on indexed file source, this should not happen")
	}
	for {
		base, lib, hasIndex := s.blockIndexManager.NextMergedBlocksBase()
		if !hasIndex {
			libString := ""
			if lib != nil {
				libString = lib.String()
			}
			s.logger.Debug("indexed file source switching to next source", zap.Uint64("base", base), zap.String("lib", libString))
			nextHandler := s.unindexedHandlerFactory(s.handler, lib)
			nextSource := s.unindexedSourceFactory(base, nextHandler)
			nextSource.OnTerminated(func(err error) {
				s.Shutdown(err)
			})
			nextSource.Run()
			return nextSource.Err()
		}

		s.logger.Debug("indexed file source starting a file source, backed by index", zap.Uint64("base", base))

		var options []FileSourceOption
		if len(s.blockStores) > 1 {
			options = append(options, FileSourceWithSecondaryBlocksStores(s.blockStores[1:]))
		}
		fs := NewFileSource(s.chainConfig, s.blockStores[0], base, 1, s.preprocessBlock, HandlerFunc(s.WrappedProcessBlock), options...)
		s.OnTerminating(func(err error) {
			fs.Shutdown(err)
		})

		err := fs.run()

		if errors.Is(err, SkipToNextRange) {
			continue
		}
		if errors.Is(err, NoMoreIndex) {
			continue
		}
		return err
	}
}

func (s *IndexedFileSource) preprocessBlock(blk *Block) (interface{}, error) {
	if s.blockIndexManager.Skip(blk) {
		return SkipThisBlock, nil
	}
	if s.preprocFunc == nil {
		return nil, nil
	}
	return s.preprocFunc(blk)
}

func safeMinus(i, j uint64) uint64 {
	if i < j {
		return 0
	}
	return i - j
}

func (s *IndexedFileSource) WrappedProcessBlock(blk *Block, obj interface{}) error {
	if err, ok := obj.(error); ok && errors.Is(err, SkipThisBlock) {
		s.skipCount++
		if s.skipCount%10 == 0 {
			nextBase, _, hasIndex := s.blockIndexManager.NextMergedBlocksBase()
			if hasIndex && safeMinus(nextBase, blk.Number) > 200 {
				return SkipToNextRange
			}
		}
		return nil
	}

	// dedupe blocks from filesource (we don't dedupe in preprocess because of multithreading complexity)
	if s.lastProcessed != nil && blk.Number <= s.lastProcessed.Num() {
		return nil
	}
	s.lastProcessed = blk

	ppblk := &PreprocessedBlock{
		blk,
		obj,
	}
	_, indexedRangeComplete, err := s.blockIndexManager.ProcessOrderedSegment(ppblk, s) // s.ProcessBlock will be called from there
	if err != nil {
		return err
	}

	if indexedRangeComplete {
		return NoMoreIndex
	}
	return nil
}

func (s *IndexedFileSource) ProcessBlock(blk *Block, obj interface{}) error {
	bRef := blk.AsRef()

	if s.sendNew {
		var skip bool
		if s.cursor != nil {
			lastCursorSentNewBlock := s.cursor.Block.Num()
			if s.cursor.Step == StepIrreversible {
				lastCursorSentNewBlock = s.cursor.HeadBlock.Num()
			}

			if blk.Number <= lastCursorSentNewBlock {
				skip = true
			}
		}
		if !skip {
			if err := s.handler.ProcessBlock(blk, s.wrapObjectWithCursor(obj, bRef, StepNew)); err != nil {
				return err
			}
		}
	}

	if s.sendIrr {
		var skip bool
		if s.cursor != nil {
			cursorLIBSent := s.cursor.Step == StepIrreversible || s.cursor.Step == StepNew && s.cursor.Block.Num() > s.cursor.LIB.Num()
			if blk.Number < s.cursor.LIB.Num() || (blk.Number == s.cursor.LIB.Num() && cursorLIBSent) {
				skip = true
			}
		}
		if !skip {
			if err := s.handler.ProcessBlock(blk, s.wrapObjectWithCursor(obj, bRef, StepIrreversible)); err != nil {
				return err
			}
		}
	}

	if s.cursor != nil && blk.Num() > s.cursor.HeadBlock.Num() {
		s.cursor = nil
	}

	return nil
}

func (s *IndexedFileSource) wrapObjectWithCursor(obj interface{}, blk BlockRef, step StepType) *wrappedObject {
	headBlock := blk
	if s.cursor != nil && s.cursor.HeadBlock.Num() > blk.Num() {
		headBlock = s.cursor.HeadBlock
	}
	return &wrappedObject{
		cursor: &Cursor{
			Step:      step,
			Block:     blk,
			LIB:       blk,
			HeadBlock: headBlock,
		},
		obj: obj,
	}
}

type wrappedObject struct {
	obj    interface{}
	cursor *Cursor
}

func (w *wrappedObject) Step() StepType {
	return w.cursor.Step
}

func (w *wrappedObject) WrappedObject() interface{} {
	return w.obj
}

func (w *wrappedObject) Cursor() *Cursor {
	return w.cursor
}
