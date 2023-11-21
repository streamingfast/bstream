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
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type EternalSourceOption = func(s *EternalSource)

func EternalSourceWithLogger(logger *zap.Logger) EternalSourceOption {
	return func(s *EternalSource) {
		s.logger = logger
	}
}

type EternalSourceStartBackAtBlock func() (BlockRef, error)

var eternalRestartWaitTime = time.Second * 2

type EternalSource struct {
	*shutter.Shutter
	sourceFromRefFactory SourceFromRefFactory
	h                    Handler
	startBackAt          EternalSourceStartBackAtBlock
	currentSource        Source
	restartDelay         time.Duration

	logger *zap.Logger
}

func NewEternalSource(sf SourceFromRefFactory, h Handler, opts ...EternalSourceOption) *EternalSource {
	es := &EternalSource{
		sourceFromRefFactory: sf,
		h:                    h,
		restartDelay:         eternalRestartWaitTime,
		logger:               zlog,
	}

	for _, opt := range opts {
		opt(es)
	}

	es.Shutter = shutter.New()
	es.Shutter.OnTerminating(func(err error) {
		if es.currentSource != nil {
			es.currentSource.Shutdown(err)
		}
	})

	return es
}

func NewDelegatingEternalSource(sf SourceFromRefFactory, startBackAt EternalSourceStartBackAtBlock, h Handler, opts ...EternalSourceOption) *EternalSource {
	es := NewEternalSource(sf, h, opts...)
	es.startBackAt = startBackAt

	return es
}

func (s *EternalSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *EternalSource) Run() {
	var lastProcessedBlockRef BlockRef = BlockRefEmpty
	handler := s.h

	// When `startBackAt` is **not** defined, we simply use an handler that record the last processed block ref that is feed upon restart
	if s.startBackAt == nil {
		handler = HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
			err := s.h.ProcessBlock(blk, obj)
			if err != nil {
				return err
			}

			lastProcessedBlockRef = NewBlockRef(blk.Id, blk.Number)
			return nil
		})
	}

	var err error
	for {
		if s.IsTerminating() {
			return
		}
		s.logger.Info("starting run loop")

		if s.startBackAt != nil {
			lastProcessedBlockRef, err = s.startBackAt()
			if err != nil {
				s.onEternalSourceTermination(fmt.Errorf("failed to get start at block ref: %w", err))
				return
			}
		}

		s.logger.Debug("calling sourceFromRefFactory", zap.Stringer("last_processed_block", lastProcessedBlockRef))
		src := s.sourceFromRefFactory(lastProcessedBlockRef, handler)
		s.currentSource = src // we'll lock you some day
		src.Run()

		<-src.Terminating()
		s.onEternalSourceTermination(src.Err())
	}
}

func (s *EternalSource) onEternalSourceTermination(err error) {
	if err != nil {
		s.logger.Info("eternal source failed", zap.Error(err))
	}

	s.logger.Info("sleeping before restarting underlying source", zap.Duration("wait_time", s.restartDelay))
	time.Sleep(s.restartDelay)
}
