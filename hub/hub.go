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
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"go.uber.org/zap"
)

// SubscriptionHub hooks to a live data source
type SubscriptionHub struct {
	initialStartBlockNum uint64

	buffer   *bstream.Buffer // Locked through `subscribersLock`.
	tailLock *bstream.TailLock

	subscribers     []*subscriber
	subscribersLock sync.Mutex // Locks `buffer` reads and writes

	fileSourceFactory bstream.SourceFromNumFactory
	liveSourceFactory bstream.SourceFromNumFactory

	sourceChannelSize int
	realtimeTolerance time.Duration
	realtimePassed    chan struct{}
	tailLockFunc      TailLockFunc

	skipMemoization bool

	logger *zap.Logger
}

type TailLockFunc func(tailBlockNum uint64) (func(), error)

func NewSubscriptionHub(startBlock uint64, buffer *bstream.Buffer, tailLockFunc TailLockFunc, fileSourceFactory bstream.SourceFromNumFactory, liveSourceFactory bstream.SourceFromNumFactory, opts ...Option) (*SubscriptionHub, error) {
	h := &SubscriptionHub{
		initialStartBlockNum: startBlock,
		fileSourceFactory:    fileSourceFactory,
		liveSourceFactory:    liveSourceFactory,
		buffer:               buffer,
		tailLockFunc:         tailLockFunc,
		realtimePassed:       make(chan struct{}),
		realtimeTolerance:    time.Second * 15,
		sourceChannelSize:    100, // default value, use Option to change it
		logger:               zlog,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
}

// WaitUntilRealTime waits until the hub reaches a live real-time block which
// is a block whose block time is within the realtime tolerance threshold sets
// on the hub config.
//
// It waits until the real-time block is within realtime tolerance threshold or
// the context is done, whichever arrives first.
func (h *SubscriptionHub) WaitUntilRealTime(ctx context.Context) {
	select {
	case <-h.realtimePassed:
	case <-ctx.Done():
	}
}

// WaitReady waits until the hub reaches a live block which is a block
// whose block time is within the realtime tolerance threshold sets on this
// hub config.
//
// Deprecated: Use `WaitUntilRealTime(context.TODO())` instead.
func (h *SubscriptionHub) WaitReady() { h.WaitUntilRealTime(context.Background()) }

func (h *SubscriptionHub) HeadBlockID() string {
	head := h.buffer.Head()
	if head == nil {
		return ""
	}
	return head.ID()
}

func (h *SubscriptionHub) HeadBlock() bstream.BlockRef {
	head := h.buffer.Head()
	if head == nil {
		return nil
	}
	return head
}

func (h *SubscriptionHub) HeadTracker(_ context.Context) (bstream.BlockRef, error) {
	res := h.HeadBlock()
	if res == nil {
		return nil, bstream.ErrTrackerBlockNotFound
	}
	return res, nil
}

func (h *SubscriptionHub) Launch() {

	hubHandler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		start := time.Now()
		var parsingEnd time.Time
		var bufferDuration time.Duration
		var zFields []zap.Field
		defer func() {
			t := time.Since(start)
			if t > 400*time.Millisecond {
				zFields = append(zFields, zap.Duration("block_processing_duration", t))
				zFields = append(zFields, zap.Duration("block_parsing_duration", parsingEnd.Sub(start)))
				zFields = append(zFields, zap.Duration("buffer_push_back", bufferDuration))
				zFields = append(zFields, zap.Uint64("block_num", blk.Num()))

				h.logger.Info("hub is overloaded", zFields...) // alerting is done on consequences of this, instead
			}
		}()

		if !h.skipMemoization {
			// The `ToNative` call is memoized and **removes the original payload**
			// ensure that all consumer of this block get the same decoded instance
			// or clone the block before you call ToNative() in your handler
			blk.ToNative()
		}

		parsingEnd = time.Now()
		lockStart := time.Now()

		var children []*subscriber
		var preprocBlock *bstream.PreprocessedBlock
		func() {
			h.subscribersLock.Lock()
			defer h.subscribersLock.Unlock()

			zFields = append(zFields, zap.Duration("get_lock", time.Since(lockStart)))

			children = h.subscribers

			bufferStart := time.Now()
			preprocBlock = &bstream.PreprocessedBlock{Block: blk, Obj: obj}

			h.buffer.AppendHead(preprocBlock) // Truncation is managed by the TailManager

			bufferDuration = time.Since(bufferStart)
		}()

		zFields = append(zFields, zap.Duration("total_lock_duration", time.Since(lockStart)))

		for _, sub := range children {
			subStart := time.Now()
			if len(sub.input) == cap(sub.input) {
				h.logger.Warn("hub shutting down subscriber source, it's at max capacity, unable to add more blocks to it", zap.Int("chan_capacity", cap(sub.input)))
				sub.Shutdown(fmt.Errorf("shutting down subscriber before it goes over capacity"))
				continue
			}
			if sub.passedGracePeriod && len(sub.input) >= h.sourceChannelSize {
				h.logger.Warn("hub shutting down subscriber source, it is over desired chan size and grace period over", zap.Int("current_length", len(sub.input)), zap.Int("target_max_channel_size", h.sourceChannelSize), zap.Int("chan_capacity", cap(sub.input)))
				sub.Shutdown(fmt.Errorf("shutting down subscriber before it goes over capacity"))
				continue
			}
			sub.input <- preprocBlock
			zFields = append(zFields, zap.Duration(reflect.TypeOf(sub).String(), time.Since(subStart)))
		}

		return nil
	})

	realtimeTripper := bstream.NewRealtimeTripper(h.realtimeTolerance, func() {
		close(h.realtimePassed)
	}, hubHandler)

	sf := bstream.SourceFromRefFactory(func(startRef bstream.BlockRef, handler bstream.Handler) bstream.Source {
		effectiveHandler := handler
		startBlockNum := uint64(0)

		if startRef != nil && startRef.ID() != "" {
			startBlockNum = startRef.Num()

			h.logger.Info("joining source block id gate creation", zap.Stringer("start_block", startRef))
			effectiveHandler = bstream.NewBlockIDGate(startRef.ID(), bstream.GateInclusive, handler, bstream.GateOptionWithLogger(h.logger))
		} else {
			startBlockNum = h.initialStartBlockNum

			h.logger.Info("joining source block num gate creation", zap.Uint64("start_block_num", startBlockNum))
			effectiveHandler = bstream.NewBlockNumGate(startBlockNum, bstream.GateInclusive, handler, bstream.GateOptionWithLogger(h.logger))
		}

		fileSourceFactory := bstream.SourceFactory(func(handler bstream.Handler) bstream.Source {
			h.logger.Info("creating file source", zap.Uint64("start_block_num", startBlockNum))
			return h.fileSourceFactory(startBlockNum, handler)
		})

		h.logger.Info("source creation", zap.Uint64("start_block_num", startBlockNum))
		options := []bstream.JoiningSourceOption{bstream.JoiningSourceLogger(h.logger)}
		if startRef != nil && startRef.ID() != "" {
			options = append(options, bstream.JoiningSourceTargetBlockID(startRef.ID()))
		} else {
			options = append(options, bstream.JoiningSourceTargetBlockNum(bstream.GetProtocolFirstStreamableBlock))
		}

		liveSourceFactory := bstream.SourceFactory(func(handler bstream.Handler) bstream.Source {
			return h.liveSourceFactory(startBlockNum, handler)
		})

		return bstream.NewJoiningSource(fileSourceFactory, liveSourceFactory, effectiveHandler, options...)
	})

	es := bstream.NewEternalSource(sf, realtimeTripper)
	es.Run()
	es.OnTerminating(func(e error) {
		h.logger.Error("shutdown, quiting ...", zap.Error(e))
	})
}

// NewSource issues new sources fed from the Hub.
func (h *SubscriptionHub) NewSource(handler bstream.Handler, burst int) bstream.Source {
	source := newHubSourceWithBurst(h, handler, burst)
	source.SetLogger(h.logger.Named("source"))

	return source
}

func (h *SubscriptionHub) NewSourceFromBlockRef(ref bstream.BlockRef, handler bstream.Handler) bstream.Source {
	return h.NewSourceFromBlockNumWithOpts(ref.Num(), handler, bstream.JoiningSourceTargetBlockID(ref.ID()))
}

func (h *SubscriptionHub) NewSourceFromBlockNum(blockNum uint64, handler bstream.Handler) bstream.Source {
	return h.NewSourceFromBlockNumWithOpts(blockNum, handler)
}

func (h *SubscriptionHub) NewHubSourceFromBlockNum(blockNum uint64, handler bstream.Handler) (*HubSource, error) {
	releaseFunc, err := h.tailLockFunc(blockNum)
	if err != nil {
		return nil, fmt.Errorf("fail to lock hub's buffer for block (%d): %w", blockNum, err)
	}
	return newHubSourceFromBlockNum(h, handler, blockNum, releaseFunc), nil
}

func (h *SubscriptionHub) NewSourceFromBlockNumWithOpts(blockNum uint64, handler bstream.Handler, opts ...bstream.JoiningSourceOption) bstream.Source {
	releaseFunc, err := h.tailLockFunc(blockNum)
	if err == nil {
		return newHubSourceFromBlockNum(h, handler, blockNum, releaseFunc)
	}

	fileFactory := func(handler bstream.Handler) bstream.Source {
		return h.fileSourceFactory(blockNum, handler)
	}

	liveFactory := func(handler bstream.Handler) bstream.Source {
		return newHubSourceWithBurst(h, handler, 300)
	}

	return bstream.NewJoiningSource(fileFactory, liveFactory, handler, append(opts, bstream.JoiningSourceLogger(h.logger))...)
}

type subscriber struct {
	input             chan *bstream.PreprocessedBlock
	passedGracePeriod bool // allows blocks to go in channel even if len > h.sourceChannelSize
	Shutdown          func(error)

	logger *zap.Logger
}

func (h *SubscriptionHub) prefillSubscriberAtBlockNum(sub *subscriber, startBlockNum uint64) (err error) {
	// TODO: DRY up this func and the other `prefill`.. mucho duplication.
	sub.input = make(chan *bstream.PreprocessedBlock, h.buffer.Len()+h.sourceChannelSize)

	start := time.Now()

	sub.logger.Debug("filling subscriber at block num", zap.Uint64("start_block_num", startBlockNum), zap.Int("chan_capacity", cap(sub.input)), zap.Int("target_max_channel_size", h.sourceChannelSize))
	var seenStartBlock bool
	for _, blk := range h.buffer.AllBlocks() {
		num := blk.Num()

		if num < startBlockNum {
			continue
		}

		if num == startBlockNum {
			seenStartBlock = true
		}

		if num > startBlockNum && !seenStartBlock {
			return fmt.Errorf("hub souce didn't provide startBlockNum, perhaps truncation happened between fetching HubSource and Running it")
		}

		preprocessedBlk := blk.(*bstream.PreprocessedBlock)
		if len(sub.input) == cap(sub.input) {
			sub.logger.Warn("burst to block failed, channel full", zap.Uint64("start_block_num", startBlockNum))
			return fmt.Errorf("channel full")
		}
		sub.input <- preprocessedBlk
	}

	sub.logger.Debug("burst to block ended", zap.Duration("execution_time", time.Since(start)), zap.Uint64("start_block_num", startBlockNum))

	go scheduleEndOfGracePeriod(sub)

	return nil
}

func scheduleEndOfGracePeriod(sub *subscriber) {
	time.Sleep(10 * time.Second)
	sub.logger.Debug("subscriber out of grace period", zap.Int("current_length", len(sub.input)), zap.Int("chan_capacity", cap(sub.input)))
	sub.passedGracePeriod = true
}

func (h *SubscriptionHub) prefillSubscriberWithBurst(sub *subscriber, burst int) (err error) {
	sub.input = make(chan *bstream.PreprocessedBlock, burst+h.sourceChannelSize)

	start := time.Now()
	sub.logger.Debug("filling subscriber with burst", zap.Int("chan_capacity", cap(sub.input)), zap.Int("target_max_channel_size", h.sourceChannelSize), zap.Int("burst", burst))

	for _, blk := range h.buffer.HeadBlocks(burst) {
		preprocessedBlk := blk.(*bstream.PreprocessedBlock)
		if len(sub.input) == cap(sub.input) {
			sub.logger.Warn("burst by size failed, channel full", zap.Int("burst_size", burst), zap.Int("chan_capacity", cap(sub.input)))
			return fmt.Errorf("channel full")
		}
		sub.input <- preprocessedBlk
	}

	sub.logger.Debug("burst by size ended", zap.Duration("execution_time", time.Since(start)), zap.Int("burst_size", burst))

	go scheduleEndOfGracePeriod(sub)

	return nil
}

// unsubscribe is called from the source.OnTerminating() func
func (h *SubscriptionHub) unsubscribe(removeSub *subscriber) {
	h.subscribersLock.Lock()
	defer h.subscribersLock.Unlock()

	var newSubscriber []*subscriber
	for _, sub := range h.subscribers {
		if sub != removeSub {
			newSubscriber = append(newSubscriber, sub)
		}
	}
	h.subscribers = newSubscriber
}
