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

	"github.com/dfuse-io/bstream"
	"go.uber.org/zap"
)

func newSubscription(chanSize int) (out *subscription) {
	return &subscription{
		incomingBlock: make(chan *bstream.Block, chanSize),
	}
}

type subscription struct {
	quitOnce sync.Once
	closed   bool

	incomingBlock chan *bstream.Block
	subscriber    string
}

func (s *subscription) SetSubscriber(subscriber string) {
	s.subscriber = subscriber
}

func (s *subscription) Push(blk *bstream.Block) {
	if len(s.incomingBlock) == cap(s.incomingBlock) {
		s.quitOnce.Do(func() {
			zlog.Info("reach max buffer size for subcription, closing channel", zap.String("subscriber", s.subscriber), zap.Int("capacity", cap(s.incomingBlock)))
			s.closed = true
			close(s.incomingBlock)
		})
		return
	}

	if s.closed {
		zlog.Info("Warning: Pushing block in a close subscription", zap.String("subscriber", s.subscriber), zap.Int("capacity", cap(s.incomingBlock)))
		return
	}

	zlog.Debug("subscription writing accepted block", zap.String("subscriber", s.subscriber), zap.String("subscriber", s.subscriber))
	s.incomingBlock <- blk
}
