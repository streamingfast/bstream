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
	"errors"
	"fmt"
	"github.com/streamingfast/dtracing"
	"sync"
	"time"

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

	fileSourceFactory ForkableSourceFactory
	liveSourceFactory ForkableSourceFactory

	lowestLiveBlockNum uint64
	liveSource         Source
	sourcesLock        sync.Mutex

	handler Handler

	lastBlockProcessed *Block

	ctx           context.Context
	startBlockNum uint64 // overriden by cursor if it exists
	cursor        *Cursor

	logger *zap.Logger
}

func NewJoiningSource(
	fileSourceFactory,
	liveSourceFactory ForkableSourceFactory,
	h Handler,
	ctx context.Context,
	startBlockNum uint64,
	cursor *Cursor,
	logger *zap.Logger) *JoiningSource {
	logger.Info("creating new joining source", zap.Stringer("cursor", cursor), zap.Uint64("start_block_num", startBlockNum))

	s := &JoiningSource{
		Shutter:           shutter.New(),
		fileSourceFactory: fileSourceFactory,
		liveSourceFactory: liveSourceFactory,
		handler:           h,
		ctx:               ctx,
		startBlockNum:     startBlockNum,
		cursor:            cursor,
		logger:            logger,
	}

	return s
}

func (s *JoiningSource) Run() {
	s.Shutdown(s.run())
}

func (s *JoiningSource) run() error {

	// if liveSource works, no need for fileSource or wrapped handler
	if src := s.tryGetSource(s.handler, s.liveSourceFactory); src != nil {
		s.liveSource = src

		s.OnTerminating(s.liveSource.Shutdown)
		s.liveSource.Run()
		return s.liveSource.Err()
	}
	if lowestBlockGetter, ok := s.liveSourceFactory.(LowSourceLimitGetter); ok {
		s.lowestLiveBlockNum = lowestBlockGetter.LowestBlockNum()
	}

	fileSrc := s.tryGetSource(HandlerFunc(s.fileSourceHandler), s.fileSourceFactory)

	if fileSrc == nil {
		return fmt.Errorf("cannot run joining_source: start_block %d (cursor %s) not found",
			s.startBlockNum,
			s.cursor.String())
	}

	defer s.deleteBlocksBehindLive()

	s.OnTerminating(fileSrc.Shutdown)
	fileSrc.Run()

	if s.liveSource == nil { // got stopped before joining
		return fileSrc.Err()
	}

	s.OnTerminating(s.liveSource.Shutdown)
	s.liveSource.Run()
	return s.liveSource.Err()
}

func (s *JoiningSource) tryGetSource(handler Handler, factory ForkableSourceFactory) Source {
	if s.cursor != nil {
		return factory.SourceFromCursor(s.cursor, handler)
	}
	return factory.SourceFromBlockNum(s.startBlockNum, handler)
}

func (s *JoiningSource) fileSourceHandler(blk *Block, obj interface{}) error {
	if s.liveSource != nil { // we should be already shutdown anyway
		return nil
	}

	s.logBlocksBehindLive(s.lowestLiveBlockNum - blk.Number)

	if blk.Number >= s.lowestLiveBlockNum {
		if src := s.liveSourceFactory.SourceFromBlockNum(blk.Number, s.handler); src != nil {
			s.liveSource = src
			return stopSourceOnJoin
		}
		if lowestBlockGetter, ok := s.liveSourceFactory.(LowSourceLimitGetter); ok {
			s.lowestLiveBlockNum = lowestBlockGetter.LowestBlockNum()
		}
	}

	return s.handler.ProcessBlock(blk, obj)
}

func (s *JoiningSource) deleteBlocksBehindLive() {
	traceId := dtracing.GetTraceIDOrEmpty(s.ctx).String()
	s.logger.Debug("delete blocks behind live metric", zap.String("trace_id", traceId))
	go func() {
		time.Sleep(2 * time.Minute)
		BlocksBehindLive.DeleteLabelValues(traceId)
	}()
}

func (s *JoiningSource) logBlocksBehindLive(blocksBehindLive uint64) {
	traceId := dtracing.GetTraceIDOrEmpty(s.ctx).String()

	// if we caught up we don't need to keep the metric anymore
	if blocksBehindLive <= 0 {
		s.deleteBlocksBehindLive()
	} else {
		BlocksBehindLive.SetUint64(blocksBehindLive, traceId)
	}
}
