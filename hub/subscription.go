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
)

// Subscription is a bstream.Source and has the following guarantees:
type Subscription struct {
	*shutter.Shutter
	handler     bstream.Handler
	blocks chan *bstream.PreprocessedBlock
}

//			s.hub.unsubscribe(sub)
func NewSubscription(handler bstream.Handler, chanSize int) *Subscription {
	sub := &Subscription{
		Shutter: shutter.New(),
		handler: handler,
		blocks:  make(chan *bstream.PreprocessedBlock, chanSize),
	}

	return sub
}

func (s *Subscription) push(ppblk *bstream.PreprocessedBlock) error {
	if len(s.blocks) == cap(s.blocks) {
		return fmt.Errorf("subscription channel at max capacity")
	}
	s.blocks <- ppblk
	return nil
}

func (s *Subscription) run() error {
	for {
		select {
		case ppblk := <-s.blocks:
			if s.IsTerminating() { // deal with non-predictibility of select
				return nil
			}
			if err := s.handler.ProcessBlock(ppblk.Block, ppblk.Obj); err != nil {
				return err
			}
		case <-s.Terminating():
			return nil
		}
	}
}

func (s *Subscription) Run() {
	s.Shutdown(s.run())
}
