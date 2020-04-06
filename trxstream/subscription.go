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

package trxstream

import (
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"sync"

	"go.uber.org/zap"
)

func newSubscription(chanSize int) (out *subscription) {
	return &subscription{
		incomingTrx: make(chan *pbbstream.Transaction, chanSize),
	}
}

type subscription struct {
	name        string
	incomingTrx chan *pbbstream.Transaction
	closed      bool
	quitOnce    sync.Once
}

func (s *subscription) SetName(name string) {
	s.name = name
}

func (s *subscription) Push(trx *pbbstream.Transaction) {
	if len(s.incomingTrx) == cap(s.incomingTrx) {
		s.quitOnce.Do(func() {
			zlog.Info("reach max buffer size for subscription, closing channel", zap.String("name", s.name))
			s.closed = true
			close(s.incomingTrx)
		})
		return
	}

	if s.closed {
		zlog.Warn("received trx in a close subscription")
		return
	}

	zlog.Debug("subscription writing accepted block", zap.String("name", s.name), zap.Int("capacity", cap(s.incomingTrx)))
	s.incomingTrx <- trx
}
