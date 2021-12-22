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

package hub

import (
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// HubSource has the following guarantees:
// * if a ProcessBlock() returns an error to this source, ProcessBlock will not be called again by this source.
// * if a ProcessBlock() calls source.Shutdown(), the source will not call ProcessBlock again. (this was an issue with BufferedHandler solution)
// WARNING: because of that second guarantee, allowing ProcessBlock to call its own source's shutdown function, you should never assume that there is no ongoing or incoming ProcessBlock() when calling source.Shutdown() from another thread. Don't do anything dangerous like closing a channel awaiting a Write from that source! Let the channel die from garbage collecting, wrap your reads and writes with select() on context.Terminating() or source.Terminating() and so on.
type HubSource struct {
	*shutter.Shutter
	handler          bstream.Handler
	preBufferHandler bstream.Handler
	hub              *SubscriptionHub
	startBlockNum    uint64
	burst            int
	releaseFunc      func()

	logger *zap.Logger
}

func newHubSourceFromBlockNum(hub *SubscriptionHub, handler bstream.Handler, blockNum uint64, releaseFunc func()) *HubSource {
	hs := &HubSource{
		hub:           hub,
		Shutter:       shutter.New(),
		startBlockNum: blockNum,
		releaseFunc:   releaseFunc,
		handler:       handler,
		logger:        hub.logger.Named(fmt.Sprintf("source-%d", blockNum)),
	}

	hs.OnTerminating(func(_ error) {
		releaseFunc()
	})
	return hs
}

func newHubSourceWithBurst(hub *SubscriptionHub, handler bstream.Handler, burst int) *HubSource {
	return &HubSource{
		hub:     hub,
		burst:   burst,
		Shutter: shutter.New(),
		handler: handler,
		logger:  zlog,
	}
}

func (s *HubSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *HubSource) Run() {
	sub := &subscriber{
		logger:   s.logger.Named("subscriber"),
		Shutdown: s.Shutdown,
	}

	err := s.LockedInit(func() (err error) {
		s.hub.subscribersLock.Lock()
		defer s.hub.subscribersLock.Unlock()

		if s.startBlockNum > 0 {
			err = s.hub.prefillSubscriberAtBlockNum(sub, s.startBlockNum)
		} else {
			err = s.hub.prefillSubscriberWithBurst(sub, s.burst)
		}
		if err != nil {
			return
		}
		s.hub.subscribers = append(s.hub.subscribers, sub)

		if s.releaseFunc != nil {
			s.releaseFunc()
		}

		s.OnTerminating(func(_ error) {
			// this cannot be set before calling attachHandler
			// because it could cause à source Shutdown in some cases
			s.logger.Debug("hub source is shutting down")
			s.hub.unsubscribe(sub)
		})
		return
	})

	if err != nil {
		s.Shutdown(err)
		return
	}

	go func() {
		for {
			select {
			case ppblk := <-sub.input:
				if s.IsTerminating() { // deal with non-predictibility of select
					return
				}

				// MARK THE BLOCK DATA AS SHARED:
				//ppblk.Block.Native.MarkShared()

				err := s.handler.ProcessBlock(ppblk.Block, ppblk.Obj)
				if err != nil {
					s.Shutdown(err)
					return
				}
			case <-s.Terminating():
				return
			}
		}
	}()

	<-s.Terminating()
}
