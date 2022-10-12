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
	"strings"
	"time"

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

	forkable *forkable.Forkable

	keepFinalBlocks int

	optionalHandler   bstream.Handler
	subscribers       []*Subscription
	sourceChannelSize int

	ready bool
	Ready chan struct{}

	liveSourceFactory                  bstream.SourceFactory
	oneBlocksSourceFactory             bstream.SourceFromNumFactory
	oneBlocksSourceFactoryWithSkipFunc bstream.SourceFromNumFactoryWithSkipFunc
}

func NewForkableHub(liveSourceFactory bstream.SourceFactory, oneBlocksSourceFactory interface{}, keepFinalBlocks int, extraForkableOptions ...forkable.Option) *ForkableHub {
	hub := &ForkableHub{
		Shutter:           shutter.New(),
		liveSourceFactory: liveSourceFactory,
		keepFinalBlocks:   keepFinalBlocks,
		sourceChannelSize: 100, // number of blocks that can add up before the subscriber processes them
		Ready:             make(chan struct{}),
	}

	switch fact := oneBlocksSourceFactory.(type) {
	case bstream.SourceFromNumFactoryWithSkipFunc:
		hub.oneBlocksSourceFactoryWithSkipFunc = fact
	case bstream.SourceFromNumFactory:
		hub.oneBlocksSourceFactory = fact
	default:
		panic("invalid oneBlocksSourceFactory interface")
	}

	hub.forkable = forkable.New(bstream.HandlerFunc(hub.processBlock),
		forkable.HoldBlocksUntilLIB(),
		forkable.WithKeptFinalBlocks(keepFinalBlocks),
	)

	for _, opt := range extraForkableOptions {
		opt(hub.forkable)
	}

	hub.OnTerminating(func(err error) {
		for _, sub := range hub.subscribers {
			sub.Shutdown(err)
		}
	})

	return hub
}

func (h *ForkableHub) LowestBlockNum() uint64 {
	if h != nil && h.ready {
		return h.forkable.LowestBlockNum()
	}
	return 0
}

func (h *ForkableHub) HeadInfo() (headNum uint64, headID string, headTime time.Time, libNum uint64, err error) {
	if h != nil && h.ready {
		return h.forkable.HeadInfo()
	}
	err = fmt.Errorf("not ready")
	return
}

func (h *ForkableHub) HeadNum() uint64 {
	if h != nil && h.ready {
		return h.forkable.HeadNum()
	}
	return 0
}
func (h *ForkableHub) MatchSuffix(req string) bool {
	ids := h.forkable.AllIDs()
	for _, id := range ids {
		if strings.HasSuffix(id, req) {
			return true
		}
	}
	return false
}

func (h *ForkableHub) IsReady() bool {
	return h.ready
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

func (h *ForkableHub) SourceFromBlockNum(num uint64, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromNum(num, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	}, false)
	if err != nil {
		zlog.Debug("error getting source_from_block_num", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceFromBlockNumWithForks(num uint64, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromNum(num, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	}, true)
	if err != nil {
		zlog.Debug("error getting source_from_block_num", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceFromCursor(cursor *bstream.Cursor, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromCursor(cursor, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks)
	})
	if err != nil {
		zlog.Debug("error getting source_from_cursor", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) bootstrap(blk *bstream.Block) error {

	// don't try bootstrapping from one-block-files if we are not at HEAD
	if blk.Num() < h.forkable.HeadNum() {
		return h.forkable.ProcessBlock(blk, nil)
	}

	if !h.forkable.Linkable(blk) {
		startBlock := substractAndRoundDownBlocks(blk.LibNum, uint64(h.keepFinalBlocks))

		var oneBlocksSource bstream.Source
		if h.oneBlocksSourceFactoryWithSkipFunc != nil {
			skipFunc := func(idSuffix string) bool {
				return h.MatchSuffix(idSuffix)
			}
			oneBlocksSource = h.oneBlocksSourceFactoryWithSkipFunc(startBlock, h.forkable, skipFunc)
		} else {
			oneBlocksSource = h.oneBlocksSourceFactory(startBlock, h.forkable)
		}

		if oneBlocksSource == nil {
			zlog.Debug("no oneBlocksSource from factory, not bootstrapping hub yet")
			return nil
		}
		zlog.Info("bootstrapping ForkableHub from one-block-files", zap.Uint64("start_block", startBlock), zap.Stringer("head_block", blk))
		go oneBlocksSource.Run()
		select {
		case <-oneBlocksSource.Terminating():
			break
		case <-h.Terminating():
			return h.Err()
		}
	}

	if err := h.forkable.ProcessBlock(blk, nil); err != nil {
		return err
	}

	if !h.forkable.Linkable(blk) {
		fdb_head := h.forkable.HeadNum()
		if blk.Num() < fdb_head {
			zlog.Info("live block not linkable yet, will retry when we reach forkDB's HEAD", zap.Stringer("blk_from_live", blk), zap.Uint64("forkdb_head_num", fdb_head))
			return nil
		}
		zlog.Warn("cannot initialize forkDB from one-block-files (hole between live and one-block-files). Will retry on every incoming live block.", zap.Uint64("forkdb_head_block", fdb_head), zap.Stringer("blk_from_live", blk))
		return nil
	}
	zlog.Info("hub is now Ready")

	h.ready = true
	close(h.Ready)
	return nil
}

func (h *ForkableHub) Run() {
	liveSource := h.liveSourceFactory(bstream.HandlerFunc(h.bootstrapperHandler))
	liveSource.OnTerminating(h.reconnect)
	liveSource.Run()
}

func (h *ForkableHub) reconnect(err error) {
	failFunc := func() {
		h.Shutdown(fmt.Errorf("cannot link new blocks to chain after a reconnection"))
	}

	rh := newReconnectionHandler(
		h.forkable,
		h.forkable.HeadNum,
		time.Minute,
		failFunc,
	)

	zlog.Info("reconnecting hub after disconnection. expecting to reconnect and get blocks linking to headnum within delay",
		zap.Duration("delay", rh.timeout),
		zap.Uint64("current_head_block_num", rh.previousHeadBlock),
		zap.Error(err))

	liveSource := h.liveSourceFactory(rh)
	liveSource.OnTerminating(func(err error) {
		if rh.success {
			h.reconnect(err)
			return
		}
		failFunc()
	})
	go liveSource.Run()
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

func (h *ForkableHub) processBlock(blk *bstream.Block, obj interface{}) error {
	zlog.Debug("process_block", zap.Stringer("blk", blk), zap.Any("obj", obj.(*forkable.ForkableObject).Step()))
	preprocBlock := &bstream.PreprocessedBlock{Block: blk, Obj: obj}

	bstream.BlocksReadLiveSource.Inc()
	bstream.BytesReadLiveSource.AddInt(blk.Payload.Size())

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

type reconnectionHandler struct {
	start             time.Time
	timeout           time.Duration
	previousHeadBlock uint64
	headBlockGetter   func() uint64
	handler           bstream.Handler
	success           bool
	onFailure         func()
}

func newReconnectionHandler(
	h bstream.Handler,
	headBlockGetter func() uint64,
	timeout time.Duration,
	onFailure func(),
) *reconnectionHandler {
	return &reconnectionHandler{
		handler:           h,
		headBlockGetter:   headBlockGetter,
		previousHeadBlock: headBlockGetter(),
		timeout:           timeout,
		onFailure:         onFailure,
	}
}

func (rh *reconnectionHandler) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	if !rh.success {
		if rh.start.IsZero() {
			rh.start = time.Now()
		}
		if time.Since(rh.start) > rh.timeout {
			rh.onFailure()
			return fmt.Errorf("reconnection failed")
		}
		// head block has moved, the blocks are linkable
		if rh.headBlockGetter() > rh.previousHeadBlock {
			zlog.Info("reconnection successful")
			rh.success = true
		}
	}

	err := rh.handler.ProcessBlock(blk, obj)
	if err != nil {
		if rh.headBlockGetter() == rh.previousHeadBlock {
			rh.onFailure()
		}
		return err
	}
	return nil
}
