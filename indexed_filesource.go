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

type IndexedFileSource struct {
	shutter.Shutter

	logger        *zap.Logger
	handler       Handler
	lastProcessed BlockRef

	blockIndex BlockIndex
	blockStore dstore.Store

	nextSourceFactory func(startBlockNum uint64, h Handler, lastFromIndex BlockRef) Source // like SourceFromNum but with the last Block ... or maybe I could give a cursor ????? ohhhh SourceFromCursorFactory?

	preprocFunc PreprocessFunc
}

func (s *IndexedFileSource) Run() {
	s.Shutdown(s.run())
}

var SkipToNextRange = errors.New("skip to next range")
var SkipThisBlock struct{}

func (s *IndexedFileSource) run() error {
	for {
		base, lib, hasIndex := s.blockIndex.NextBaseBlock()
		if !hasIndex {
			s.logger.Debug("indexed file source switching to next source", zap.Uint64("base", base))
			nextSource := s.nextSourceFactory(base, s.handler, lib)
			nextSource.OnTerminated(func(err error) {
				fmt.Println("hey shuyttingdown")
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
	return HandlerFunc(func(blk *Block, obj interface{}) error {
		ppblk := &PreprocessedBlock{
			blk,
			wrapIrreversibleBlockWithCursor(blk, obj),
		}
		toProc := s.blockIndex.Reorder(ppblk)

		for _, ppblk := range toProc {
			if err := s.handler.ProcessBlock(ppblk.Block, ppblk.Obj); err != nil {
				if errors.Is(err, SkipToNextRange) {
					s.lastProcessed = ppblk
				}
				return err
			}
			s.lastProcessed = ppblk
		}
		return nil
	})
}

func wrapIrreversibleBlockWithCursor(blk *Block, obj interface{}) *wrappedObject {
	return &wrappedObject{
		cursor: &Cursor{
			Step:      StepIrreversible,
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
