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
	blockIndex BlockIndex,
	blockStore dstore.Store,
	nextSourceFactory SourceFromNumFactory,
	nextHandlerWrapper func(h Handler, lib BlockRef) Handler,
	logger *zap.Logger,
) *IndexedFileSource {
	return &IndexedFileSource{
		Shutter:            shutter.New(),
		logger:             logger,
		handler:            handler,
		preprocFunc:        preprocFunc,
		blockIndex:         blockIndex,
		blockStore:         blockStore,
		nextSourceFactory:  nextSourceFactory,
		nextHandlerWrapper: nextHandlerWrapper,
	}

}

type IndexedFileSource struct {
	*shutter.Shutter

	logger        *zap.Logger
	handler       Handler
	lastProcessed BlockRef

	blockIndex BlockIndex
	blockStore dstore.Store

	nextSourceFactory  SourceFromNumFactory
	nextHandlerWrapper func(h Handler, lib BlockRef) Handler

	preprocFunc PreprocessFunc
}

func (s *IndexedFileSource) Run() {
	s.Shutdown(s.run())
}

var SkipToNextRange = errors.New("skip to next range")
var NoMoreIndex = errors.New("no more index")

type Skippable struct{}

var SkipThisBlock = Skippable{}

func (s *IndexedFileSource) SetLogger(l *zap.Logger) {
	s.logger = l
}

func (s *IndexedFileSource) run() error {
	for {
		base, lib, hasIndex := s.blockIndex.NextBaseBlock()
		if !hasIndex {
			s.logger.Debug("indexed file source switching to next source", zap.Uint64("base", base))
			nextHandler := s.nextHandlerWrapper(s.handler, lib)
			nextSource := s.nextSourceFactory(base, nextHandler)
			nextSource.OnTerminated(func(err error) {
				s.Shutdown(err)
			})
			nextSource.Run()
			return nextSource.Err()
		}

		fs := NewFileSource(s.blockStore, base, 1, s.wrappedPreproc(), s.wrappedHandler())
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

func (s *IndexedFileSource) wrappedPreproc() PreprocessFunc {
	return func(blk *Block) (interface{}, error) {
		if s.blockIndex.Skip(blk) {
			return SkipThisBlock, nil
		}
		return s.preprocFunc(blk)
	}
}

func (s *IndexedFileSource) wrappedHandler() Handler {
	var skipCount uint64
	return HandlerFunc(func(blk *Block, obj interface{}) error {
		if _, ok := obj.(Skippable); ok { // SkipThisBlock from wrappedPreproc
			skipCount++
			if skipCount%10 == 0 {
				nextBase, _, hasIndex := s.blockIndex.NextBaseBlock()
				if hasIndex && (nextBase-blk.Number) > 200 {
					return SkipToNextRange
				}
			}
			return nil
		}

		ppblk := &PreprocessedBlock{
			blk,
			obj,
		}
		toProc, indexedRangeComplete := s.blockIndex.Reorder(ppblk)

		for _, ppblk := range toProc { // sending New, Irreversible for each block
			if err := s.handler.ProcessBlock(ppblk.Block, wrapIrreversibleBlockWithCursor(blk, ppblk.Obj, StepNew)); err != nil {
				return err
			}
			if err := s.handler.ProcessBlock(ppblk.Block, wrapIrreversibleBlockWithCursor(blk, ppblk.Obj, StepIrreversible)); err != nil {
				return err
			}
			s.lastProcessed = ppblk
		}
		if indexedRangeComplete {
			return NoMoreIndex
		}
		return nil
	})
}

func wrapIrreversibleBlockWithCursor(blk *Block, obj interface{}, step StepType) *wrappedObject {
	return &wrappedObject{
		cursor: &Cursor{
			Step:      step,
			Block:     blk.AsRef(),
			LIB:       blk.AsRef(),
			HeadBlock: blk.AsRef(),
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
