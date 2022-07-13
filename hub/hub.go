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
	"sync"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// ForkableHub gives you block Sources for blocks close to head
// it keeps reversible segment in a Forkable
// it keeps small final segment in a buffer
type ForkableHub struct {
	*shutter.Shutter
	sync.Mutex

	forkdb   *forkable.ForkDB
	forkable *forkable.Forkable

	keepFinalBlocks int

	subscribers       []*Subscription
	sourceChannelSize int

	ready                  bool
	Ready                  chan struct{}
	liveSourceFactory      bstream.SourceFactory
	oneBlocksSourceFactory bstream.SourceFromNumFactory
}

func NewForkableHub(liveSourceFactory bstream.SourceFactory, oneBlocksSourceFactory bstream.SourceFromNumFactory, keepFinalBlocks int) *ForkableHub {
	hub := &ForkableHub{
		Shutter:                shutter.New(),
		liveSourceFactory:      liveSourceFactory,
		oneBlocksSourceFactory: oneBlocksSourceFactory,
		keepFinalBlocks:        keepFinalBlocks,
		sourceChannelSize:      100, // number of blocks that can add up before the subscriber processes them
		forkdb:                 forkable.NewForkDB(),
		Ready:                  make(chan struct{}),
	}

	hub.forkable = forkable.New(hub,
		forkable.WithForkDB(hub.forkdb),
		forkable.HoldBlocksUntilLIB(),
		forkable.WithKeptFinalBlocks(keepFinalBlocks),
	)
	return hub
}

func (h *ForkableHub) LowestBlockNum() uint64 {
	if h.ready {
		return h.forkable.LowestBlockNum()
	}
	return 0
}

func (h *ForkableHub) HeadNum() uint64 {
	if h.ready {
		return h.forkable.HeadNum()
	}
	return 0
}

func (h *ForkableHub) bootstrapperHandler(blk *bstream.Block, obj interface{}) error {
	if h.ready {
		return h.forkable.ProcessBlock(blk, obj)
	}
	return h.bootstrap(blk)
}

// subscribe must be called while hub is locked
func (h *ForkableHub) subscribe(handler bstream.Handler, initialBlocks []*bstream.PreprocessedBlock) *Subscription {
	chanSize := h.sourceChannelSize + len(initialBlocks)
	sub := NewSubscription(handler, chanSize)
	for _, ppblk := range initialBlocks {
		_ = sub.push(ppblk)
	}
	h.subscribers = append(h.subscribers, sub)
	return sub
}

// unsubscribe must be called while hub is locked
func (h *ForkableHub) unsubscribe(removeSub *Subscription) {
	var newSubscriber []*Subscription
	for _, sub := range h.subscribers {
		if sub != removeSub {
			newSubscriber = append(newSubscriber, sub)
		}
	}
	h.subscribers = newSubscriber
}

func (h *ForkableHub) SourceFromBlockNum(num uint64, handler bstream.Handler) bstream.Source {
	h.Lock()
	defer h.Unlock()

	blocks := h.forkable.BlocksFromIrreversibleNum(num)
	if blocks != nil {
		return h.subscribe(handler, blocks)
	}
	return nil
}

func (h *ForkableHub) SourceFromCursor(cursor *bstream.Cursor, handler bstream.Handler) bstream.Source {
	h.Lock()
	defer h.Unlock()

	blocks := h.forkable.BlocksFromCursor(cursor)
	if blocks != nil {
		return h.subscribe(handler, blocks)
	}
	return nil
}

func (h *ForkableHub) bootstrap(blk *bstream.Block) error {

	startBlock := substractAndRoundDownBlocks(blk.LibNum, uint64(h.keepFinalBlocks))

	zlog.Info("loading blocks in ForkableHub from one-block-files", zap.Uint64("start_block", startBlock), zap.Stringer("head_block", blk))
	oneBlocksSource := h.oneBlocksSourceFactory(startBlock, h.forkable)
	go oneBlocksSource.Run()
	select {
	case <-oneBlocksSource.Terminating():
		break
	case <-h.Terminating():
		return h.Err()
	}

	if err := h.forkable.ProcessBlock(blk, nil); err != nil {
		return err
	}

	if h.forkdb.BlockInCurrentChain(blk, blk.LibNum) == bstream.BlockRefEmpty {
		zlog.Warn("cannot initialize forkDB on a final block from available one-block-files. Will keep retrying on every block before we become ready")
		return nil
	}

	h.ready = true
	close(h.Ready)
	return nil
}

func (h *ForkableHub) Run() {
	liveSource := h.liveSourceFactory(bstream.HandlerFunc(h.bootstrapperHandler))
	liveSource.OnTerminating(h.Shutdown)
	liveSource.Run()
}

func substractAndRoundDownBlocks(blknum, sub uint64) uint64 {
	var out uint64
	if blknum < sub {
		out = 0
	} else {
		out = blknum - sub
	}
	out = out / 100 * 100

	if out < bstream.GetProtocolFirstStreamableBlock {
		return bstream.GetProtocolFirstStreamableBlock
	}

	return out
}

func (h *ForkableHub) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	zlog.Debug("process_block", zap.Stringer("blk", blk), zap.Any("obj", obj.(*forkable.ForkableObject).Step()))
	preprocBlock := &bstream.PreprocessedBlock{Block: blk, Obj: obj}

	h.Lock()
	defer h.Unlock()

	subscribers := h.subscribers // we may remove some from the original slice during the loop

	for _, sub := range subscribers {
		err := sub.push(preprocBlock)
		if err != nil {
			h.unsubscribe(sub)
			sub.Shutdown(err)
		}

	}
	return nil
}
