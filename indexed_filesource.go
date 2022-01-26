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

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

func NewIndexedFileSource(
	handler Handler,
	preprocFunc PreprocessFunc,
	blockIndex BlockIndexProvider,
	blockStore dstore.Store,
	unindexedSourceFactory SourceFromNumFactory,
	unindexedHandlerFactory func(h Handler, lib BlockRef) Handler,
	logger *zap.Logger,
) *IndexedFileSource {
	return &IndexedFileSource{
		Shutter:                 shutter.New(),
		logger:                  logger,
		handler:                 handler,
		preprocFunc:             preprocFunc,
		blockIndex:              blockIndex,
		blockStore:              blockStore,
		unindexedSourceFactory:  unindexedSourceFactory,
		unindexedHandlerFactory: unindexedHandlerFactory,
	}

}

type IndexedFileSource struct {
	*shutter.Shutter

	logger        *zap.Logger
	handler       Handler
	lastProcessed BlockRef

	blockIndex BlockIndexProvider
	blockStore dstore.Store

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
	for {
		base, lib, hasIndex := s.blockIndex.NextMergedBlocksBase()
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

		fs := NewFileSource(s.blockStore, base, 1, s.preprocessBlock, HandlerFunc(s.WrappedProcessBlock))
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
	if s.blockIndex.Skip(blk) {
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
			nextBase, _, hasIndex := s.blockIndex.NextMergedBlocksBase()
			if hasIndex && safeMinus(blk.Number, nextBase) > 200 {
				return SkipToNextRange
			}
		}
		return nil
	}

	ppblk := &PreprocessedBlock{
		blk,
		obj,
	}
	lastProcessed, indexedRangeComplete, err := s.blockIndex.ProcessOrderedSegment(ppblk, s) // s.ProcessBlock will be called from there

	if lastProcessed != nil {
		s.lastProcessed = lastProcessed
	}
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
	if err := s.handler.ProcessBlock(blk, wrapObjectWithCursor(obj, bRef, StepNew)); err != nil {
		return err
	}
	if err := s.handler.ProcessBlock(blk, wrapObjectWithCursor(obj, bRef, StepIrreversible)); err != nil {
		return err
	}

	return nil

}

func wrapObjectWithCursor(obj interface{}, blk BlockRef, step StepType) *wrappedObject {
	return &wrappedObject{
		cursor: &Cursor{
			Step:      step,
			Block:     blk,
			LIB:       blk,
			HeadBlock: blk,
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
