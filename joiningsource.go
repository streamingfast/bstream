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

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var stopSourceOnJoin = errors.New("stopping source on join")

// JoiningSource joins an irreversible-only source (file) to a fork-aware source close to HEAD (live)
// 1) it tries to get the source from LiveSourceFactory (using startblock or cursor)
// 2) if it can't, it will ask the FileSourceFactory for a source of those blocks.
// 3) when it receives blocks from Filesource, it looks at LiveSource
// the JoiningSource will instantiate and run an 'initialSource' until it can bridge the gap
type JoiningSource struct {
	*shutter.Shutter

	fileSourceFactory           ForkableSourceFactory
	fileSourceHandlerMiddleware func(Handler) Handler

	liveSourceFactory           ForkableSourceFactory
	liveSourceHandlerMiddleware func(Handler) Handler

	lowestLiveBlockNum uint64
	liveSource         Source
	sourcesLock        sync.Mutex

	handler Handler

	lastBlockProcessed *pbbstream.Block

	startBlockNum  uint64 // overriden by cursor if it exists, unless we are in cursorIsTarget mode
	cursor         *Cursor
	cursorIsTarget bool

	logger *zap.Logger
}

type JoiningSourceOption func(s *JoiningSource)

func JoiningSourceWithLiveSourceHandlerMiddleware(mw func(Handler) Handler) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.liveSourceHandlerMiddleware = mw
	}
}

func JoiningSourceWithFileSourceHandlerMiddleware(mw func(Handler) Handler) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.fileSourceHandlerMiddleware = mw
	}
}

func NewJoiningSource(
	fileSourceFactory,
	liveSourceFactory ForkableSourceFactory,
	h Handler,
	startBlockNum uint64,
	cursor *Cursor,
	cursorIsTarget bool,
	logger *zap.Logger,
	opts ...JoiningSourceOption) *JoiningSource {
	logger.Debug("creating new joining source", zap.Stringer("cursor", cursor), zap.Uint64("start_block_num", startBlockNum))

	s := &JoiningSource{
		Shutter:           shutter.New(),
		fileSourceFactory: fileSourceFactory,
		liveSourceFactory: liveSourceFactory,
		handler:           h,
		startBlockNum:     startBlockNum,
		cursor:            cursor,
		cursorIsTarget:    cursorIsTarget,
		logger:            logger,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

func (s *JoiningSource) Run() {
	s.Shutdown(s.run())
}

func (s *JoiningSource) run() error {
	liveSourceHandler := s.handler
	if s.liveSourceHandlerMiddleware != nil {
		liveSourceHandler = s.liveSourceHandlerMiddleware(s.handler)
	}

	// if liveSource works, no need for fileSource or wrapped handler
	if src := s.tryGetSource(liveSourceHandler, s.liveSourceFactory); src != nil {
		s.liveSource = src

		s.OnTerminating(s.liveSource.Shutdown)
		s.liveSource.Run()
		return s.liveSource.Err()
	}
	if lowestBlockGetter, ok := s.liveSourceFactory.(LowSourceLimitGetter); ok {
		s.lowestLiveBlockNum = lowestBlockGetter.LowestBlockNum()
	}

	if err := s.checkCursorResolvable(); err != nil {
		return err
	}

	fileSrc := s.tryGetSource(HandlerFunc(s.fileSourceHandler), s.fileSourceFactory)

	if fileSrc == nil {
		return fmt.Errorf("cannot run joining_source: start_block %d (cursor %s) not found",
			s.startBlockNum,
			s.cursor.String())
	}

	s.OnTerminating(fileSrc.Shutdown)
	fileSrc.Run()

	if s.liveSource == nil { // got stopped before joining
		return fileSrc.Err()
	}

	s.OnTerminating(s.liveSource.Shutdown)
	s.liveSource.Run()
	return s.liveSource.Err()

}

func (s *JoiningSource) checkCursorResolvable() error {
	live, ok := s.liveSourceFactory.(LiveBlockKnower)
	if !ok {
		return nil
	}
	forked, _ := s.fileSourceFactory.(ForkedBlockKnower)

	return CheckCursorResolvable(s.cursor, live, forked, s.logger)
}

// CheckCursorResolvable says whether a cursor names a block that anything can still
// produce, and returns an ErrResolveCursor error when nothing can.
//
// The live source is authoritative over the range it holds: a cursor block inside that
// range whose ID it does not know is on no chain it ever saw. The one other place such a
// block can come from is the forked-blocks store — a live source restarted after the fork
// happened no longer holds it, while the store still does — so that one is asked before
// giving up.
//
// Both coming back empty is what makes a cursor unresolvable, and saying so here is what
// keeps the caller off the file source, which would otherwise wait for merged files that
// cannot contain that block: a whole bundle on a slow chain — 100 blocks, some twenty
// minutes on Ethereum — and then the same failure anyway.
func CheckCursorResolvable(cursor *Cursor, live LiveBlockKnower, forked ForkedBlockKnower, logger *zap.Logger) error {
	if cursor.IsEmpty() || live == nil {
		return nil
	}

	lowest, head := live.LowestBlockNum(), live.HeadNum()
	cursorBlockNum := cursor.Block.Num()
	if lowest == 0 || head == 0 || cursorBlockNum < lowest || cursorBlockNum > head {
		return nil
	}

	if live.GetBlockByHash(cursor.Block.ID()) != nil {
		return nil
	}

	if forked != nil {
		hasForkedBlock, err := forked.HasForkedBlock(TruncateBlockID(cursor.Block.ID()), cursorBlockNum)
		if err != nil {
			if logger != nil {
				logger.Warn("cannot look up the cursor block in the forked blocks store, leaving the cursor to the file source",
					zap.Stringer("cursor_block", cursor.Block), zap.Error(err))
			}
			return nil
		}
		if hasForkedBlock {
			return nil
		}
	}

	return fmt.Errorf("%w: block %s sits inside the live range [%d, %d], where neither the live buffer nor the forked blocks hold it",
		ErrResolveCursor, cursor.Block, lowest, head)
}

func (s *JoiningSource) tryGetSource(handler Handler, factory ForkableSourceFactory) Source {
	if s.cursor != nil {
		if s.cursorIsTarget {
			return factory.SourceThroughCursor(s.startBlockNum, s.cursor, handler)
		}
		return factory.SourceFromCursor(s.cursor, handler)
	}
	return factory.SourceFromBlockNum(s.startBlockNum, handler)
}

func (s *JoiningSource) fileSourceHandler(blk *pbbstream.Block, obj any) error {
	if s.liveSource != nil { // we should be already shutdown anyway
		return nil
	}

	liveSourceHandler := s.handler
	if s.liveSourceHandlerMiddleware != nil {
		liveSourceHandler = s.liveSourceHandlerMiddleware(s.handler)
	}

	if blk.Number >= s.lowestLiveBlockNum {
		if s.cursorIsTarget {
			if src := s.liveSourceFactory.SourceThroughCursor(blk.Number, s.cursor, liveSourceHandler); src != nil {
				s.liveSource = src
				return stopSourceOnJoin
			}
		} else {
			if src := s.liveSourceFactory.SourceFromBlockNum(blk.Number, liveSourceHandler); src != nil {
				s.liveSource = src
				return stopSourceOnJoin
			}
		}
		if lowestBlockGetter, ok := s.liveSourceFactory.(LowSourceLimitGetter); ok {
			s.lowestLiveBlockNum = lowestBlockGetter.LowestBlockNum()
		}
	}

	fileSourceHandler := s.handler
	if s.fileSourceHandlerMiddleware != nil {
		fileSourceHandler = s.fileSourceHandlerMiddleware(s.handler)
	}

	return fileSourceHandler.ProcessBlock(blk, obj)
}
