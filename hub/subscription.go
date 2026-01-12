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

var ErrSubscriptionChannelFull = fmt.Errorf("subscription channel at max capacity")

// Subscription is a bstream.Source and has the following guarantees:
type Subscription struct {
	*shutter.Shutter
	handler bstream.Handler
	blocks  chan *bstream.PreprocessedBlock
	next    *bstream.PreprocessedBlock
}

// s.hub.unsubscribe(sub)
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
		return ErrSubscriptionChannelFull
	}
	s.blocks <- ppblk
	return nil
}

func lookAhead(ch chan *bstream.PreprocessedBlock) *bstream.PreprocessedBlock {
	select {
	case ppblk := <-ch:
		return ppblk
	default:
		return nil
	}
}

// getLatestPendingVersionOfCandidateBlock checks if 'candidate' is a partial block.
// If so, it loads the next blocks in s.blocks until it finds the last partial block of that sequence or until the channel is empty.
// It returns the last partial block with the same number as the candidate. If another block was read from the channel, it is written to `s.next`.
func (s *Subscription) getLatestPendingVersionOfCandidateBlock(candidate *bstream.PreprocessedBlock) *bstream.PreprocessedBlock {

	if candidate == nil { // entrypoint
		if s.next != nil {
			candidate = s.next // from previous run
			s.next = nil
		} else if c := lookAhead(s.blocks); c != nil { // already waiting in channel
			candidate = c
		} else {
			return nil
		}
	}

	// only look for next block in a chain of "partial" blocks.
	if stepable, ok := candidate.Obj.(bstream.Stepable); ok {
		if stepable.Step() != bstream.StepPartial {
			return candidate
		}
	}

	next := lookAhead(s.blocks)
	if next == nil {
		return candidate
	}

	if next.Block.Number != candidate.Block.Number {
		s.next = next
		return candidate
	}

	// skipping 'candidate', going with 'next' and maybe next's next
	return s.getLatestPendingVersionOfCandidateBlock(next)
}

func (s *Subscription) run() error {
	var reachedLive bool
	for {
		if s.IsTerminating() {
			return nil
		}

		// here, we try to load the next block(s) from the channel to get the last of a series of "partials" of the same block
		// if nothing is found we continue to the "blocking" channel read
		next := s.getLatestPendingVersionOfCandidateBlock(nil)
		if next != nil {
			if err := s.handler.ProcessBlock(next.Block, next.Obj); err != nil {
				return err
			}
			continue
		}

		select {
		case ppblk := <-s.blocks:
			if s.IsTerminating() { // deal with non-predictibility of select
				return nil
			}

			// if we are sending to a buffered channel, make sure to remove the 'Live' property of the block
			if liveable, ok := ppblk.Obj.(bstream.Liveable); ok {
				if !reachedLive {
					if len(s.blocks) == 0 {
						reachedLive = true
					} else {
						liveable.SetLiveBlock(false)
					}
				}
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
