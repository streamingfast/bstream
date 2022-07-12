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
	"sync"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var stopSourceOnJoin = errors.New("stopping source on join")

// JoiningSource joins an irreversible-only source (left) to a fork-aware source, close to HEAD (right)
// 1) it tries to get the source from RightSourceFactory (using startblock or cursor)
// 2) if it can't, it will ask the LeftSourceFactory for a source of those blocks.
// 3) when it receives blocks from LeftSource, it looks at RightSource
// the JoiningSource will instantiate and run an 'initialSource' until it can bridge the gap
type JoiningSource struct {
	*shutter.Shutter

	leftSourceFactory  ForkableSourceFactory
	rightSourceFactory ForkableSourceFactory

	lowestRightSourceBlockNum uint64
	rightSource               Source
	sourcesLock               sync.Mutex

	handler Handler

	lastBlockProcessed *Block

	irreversibleStartBlockNum uint64 // overriden by cursor if it exists
	cursor                    *Cursor

	logger *zap.Logger
}

func NewJoiningSource(leftSourceFactory, rightSourceFactory ForkableSourceFactory, h Handler, startBlockNum uint64, cursor *Cursor, logger *zap.Logger) *JoiningSource {

	logger.Info("creating new joining source", zap.Stringer("cursor", cursor), zap.Uint64("start_block_num", startBlockNum))

	s := &JoiningSource{
		Shutter:                   shutter.New(),
		leftSourceFactory:         leftSourceFactory,
		rightSourceFactory:        rightSourceFactory,
		handler:                   h,
		irreversibleStartBlockNum: startBlockNum,
		cursor:                    cursor,
		logger:                    logger,
	}

	return s
}

func (s *JoiningSource) Run() {
	s.Shutdown(s.run())
}

func (s *JoiningSource) run() error {

	// if rightSource works, no need for leftSource or wrapped handler
	if src := tryGetSource(s.handler,
		s.rightSourceFactory,
		s.irreversibleStartBlockNum,
		s.cursor); src != nil {
		s.rightSource = src

		s.OnTerminating(s.rightSource.Shutdown)
		s.rightSource.Run()
		return s.rightSource.Err()
	}
	if lowestBlockGetter, ok := s.rightSourceFactory.(LowSourceLimitGetter); ok {
		s.lowestRightSourceBlockNum = lowestBlockGetter.LowestBlockNum()
	}

	leftSrc := tryGetSource(HandlerFunc(s.leftSourceHandler),
		s.leftSourceFactory,
		s.irreversibleStartBlockNum,
		s.cursor)

	if leftSrc == nil {
		return fmt.Errorf("cannot run joining_source: start_block %d (cursor %s) not found",
			s.irreversibleStartBlockNum,
			s.cursor.String())
	}

	s.OnTerminating(leftSrc.Shutdown)
	leftSrc.Run()

	if s.rightSource == nil { // got stopped before joining
		return leftSrc.Err()
	}

	s.OnTerminating(s.rightSource.Shutdown)
	s.rightSource.Run()
	return s.rightSource.Err()

}

func tryGetSource(handler Handler, factory ForkableSourceFactory, irrBlockNum uint64, cursor *Cursor) Source {
	if cursor != nil {
		return factory.SourceFromCursor(cursor, handler)
	}
	return factory.SourceFromBlockNum(irrBlockNum, handler)
}

func (s *JoiningSource) leftSourceHandler(blk *Block, obj interface{}) error {
	if s.rightSource != nil { // we should be already shutdown anyway
		return nil
	}

	if blk.Number >= s.lowestRightSourceBlockNum {
		if src := s.rightSourceFactory.SourceFromBlockNum(blk.Number, s.handler); src != nil {
			s.rightSource = src
			return stopSourceOnJoin
		}
		if lowestBlockGetter, ok := s.rightSourceFactory.(LowSourceLimitGetter); ok {
			s.lowestRightSourceBlockNum = lowestBlockGetter.LowestBlockNum()
		}
	}

	return s.handler.ProcessBlock(blk, obj)
}
