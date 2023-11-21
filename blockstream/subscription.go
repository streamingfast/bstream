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

package blockstream

import (
	"sync"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"go.uber.org/zap"
)

func newSubscription(chanSize int, logger *zap.Logger) (out *subscription) {
	return &subscription{
		incomingBlock: make(chan *pbbstream.Block, chanSize),
		logger:        logger,
	}
}

type subscription struct {
	quitOnce sync.Once
	closed   bool

	incomingBlock chan *pbbstream.Block

	logger *zap.Logger
}

func (s *subscription) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *subscription) Push(blk *pbbstream.Block) {
	if len(s.incomingBlock) == cap(s.incomingBlock) {
		s.quitOnce.Do(func() {
			s.logger.Info("reach max buffer size for subcription, closing channel", zap.Int("capacity", cap(s.incomingBlock)))
			s.closed = true
			close(s.incomingBlock)
		})
		return
	}

	if s.closed {
		s.logger.Info("Warning: Pushing block in a close subscription", zap.Int("capacity", cap(s.incomingBlock)))
		return
	}

	s.logger.Debug("subscription writing accepted block", zap.Int("channel_len", len(s.incomingBlock)))
	s.incomingBlock <- blk
}
