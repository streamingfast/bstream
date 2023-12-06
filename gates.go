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

	"go.uber.org/zap"
)

type Gate interface {
	SetLogger(logger *zap.Logger)
}

type GateOption func(g Gate)

func GateOptionWithLogger(logger *zap.Logger) GateOption {
	return func(g Gate) {
		g.SetLogger(logger)
	}
}

type GateType int

const (
	GateInclusive = GateType(iota)
	GateExclusive
)

func (g GateType) String() string {
	if g == GateInclusive {
		return "inclusive"
	}
	return "exclusive"
}

type BlockNumGate struct {
	blockNum uint64
	handler  Handler
	gateType GateType

	MaxHoldOff      int
	maxHoldOffCount int

	passed bool
	logger *zap.Logger
}

func NewBlockNumGate(blockNum uint64, gateType GateType, h Handler, opts ...GateOption) *BlockNumGate {
	g := &BlockNumGate{
		blockNum:   blockNum,
		gateType:   gateType,
		handler:    h,
		MaxHoldOff: 15000,
		logger:     zlog,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *BlockNumGate) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	g.passed = blk.Number >= g.blockNum

	// ex: ETH: gate could be 0, but FirstStreamable is 1, enable inclusively at 1
	// ex: EOS: gate could be 0 or 1, but FirstStreamable is 2, enable inclusively at 2
	if g.blockNum < GetProtocolFirstStreamableBlock && blk.Number == GetProtocolFirstStreamableBlock {
		g.gateType = GateInclusive
		g.passed = true
	}

	if !g.passed {
		if g.MaxHoldOff != 0 {
			g.maxHoldOffCount++
			if g.maxHoldOffCount > g.MaxHoldOff {
				return fmt.Errorf("maximum blocks held off busted: %d", g.MaxHoldOff)
			}
		}
		return nil
	}

	g.logger.Info("block num gate passed", zap.String("gate_type", g.gateType.String()), zap.Uint64("at_block_num", blk.Number), zap.Uint64("gate_block_num", g.blockNum))

	if g.gateType == GateInclusive {
		return g.handler.ProcessBlock(blk, obj)
	}
	return nil
}

func (g *BlockNumGate) SetLogger(logger *zap.Logger) {
	g.logger = logger
}

type BlockIDGate struct {
	blockID  string
	handler  Handler
	gateType GateType

	MaxHoldOff      int
	maxHoldOffCount int

	passed bool
	logger *zap.Logger
}

func NewBlockIDGate(blockID string, gateType GateType, h Handler, opts ...GateOption) *BlockIDGate {
	g := &BlockIDGate{
		blockID:    blockID,
		gateType:   gateType,
		handler:    h,
		MaxHoldOff: 15000,
		logger:     zlog,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *BlockIDGate) SetLogger(logger *zap.Logger) {
	g.logger = logger
}

func (g *BlockIDGate) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	g.passed = blk.Id == g.blockID

	if (g.blockID == "" || g.blockID == "0000000000000000000000000000000000000000000000000000000000000000") && blk.Number == 2 {
		g.gateType = GateInclusive
		g.passed = true
	}

	if !g.passed {
		if g.MaxHoldOff != 0 {
			g.maxHoldOffCount++
			if g.maxHoldOffCount > g.MaxHoldOff {
				return fmt.Errorf("maximum blocks held off busted: %d", g.MaxHoldOff)
			}
		}
		return nil
	}

	g.logger.Info("block id gate passed", zap.String("gate_type", g.gateType.String()), zap.String("block_id", g.blockID))

	if g.gateType == GateInclusive {
		return g.handler.ProcessBlock(blk, obj)
	}
	return nil
}

///////////////////////////////////////

type RealtimeGate struct {
	timeToRealtime time.Duration
	handler        Handler
	gateType       GateType

	passed bool
	logger *zap.Logger
}

func NewRealtimeGate(timeToRealtime time.Duration, h Handler, opts ...GateOption) *RealtimeGate {
	g := &RealtimeGate{
		timeToRealtime: timeToRealtime,
		handler:        h,
		logger:         zlog,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *RealtimeGate) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	blockTime := blk.Time()
	delta := time.Since(blockTime)
	g.passed = delta < g.timeToRealtime
	if !g.passed {
		return nil
	}

	g.logger.Info("realtime gate passed", zap.Duration("delta", delta))

	return g.handler.ProcessBlock(blk, obj)
}

func (g *RealtimeGate) SetLogger(logger *zap.Logger) {
	g.logger = logger
}

//////////////////////////////////////////////////

// RealtimeTripper is a pass-through handler that executes a function before
// the first block goes through.
type RealtimeTripper struct {
	passed         bool
	tripFunc       func()
	handler        Handler
	timeToRealtime time.Duration
	nowFunc        func() time.Time

	lastBlockSeenAt time.Time
	logger          *zap.Logger
}

func NewRealtimeTripper(timeToRealtime time.Duration, tripFunc func(), h Handler, opts ...GateOption) *RealtimeTripper {
	t := &RealtimeTripper{
		timeToRealtime: timeToRealtime,
		tripFunc:       tripFunc,
		handler:        h,
		nowFunc:        time.Now,
		logger:         zlog,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

func (t *RealtimeTripper) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if t.passed {
		return t.handler.ProcessBlock(blk, obj)
	}

	now := t.nowFunc()
	blockTime := blk.Time()
	delta := now.Sub(blockTime)

	t.passed = delta < t.timeToRealtime
	if t.passed {
		t.tripFunc()
		t.logger.Info("realtime tripper tripped", zap.Duration("delta", delta))
	}

	// This works well for EOS and ETH, we simply want to print the advancement when more from live source than batch of blocks.
	// Hence, if last time we seen a block, more than 0.45 elapsed, it's probably a live block.
	if !t.passed && time.Since(t.lastBlockSeenAt).Seconds() > 0.45 {
		t.logger.Info("realtime tripper seen block but still not realtime according to tolerance, waiting for realtime block to appear", zap.Stringer("block", blk.AsRef()), zap.Duration("delta", delta), zap.Duration("realtime_tolerance", t.timeToRealtime))
	}

	t.lastBlockSeenAt = now
	return t.handler.ProcessBlock(blk, obj)
}

func (t *RealtimeTripper) SetLogger(logger *zap.Logger) {
	t.logger = logger
}

// MinimalBlockNumFilter does not let anything through that is under MinimalBlockNum
type MinimalBlockNumFilter struct {
	blockNum uint64
	handler  Handler
}

func NewMinimalBlockNumFilter(blockNum uint64, h Handler) *MinimalBlockNumFilter {
	return &MinimalBlockNumFilter{
		blockNum: blockNum,
		handler:  h,
	}
}

func (f *MinimalBlockNumFilter) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if blk.Number < f.blockNum {
		return nil
	}
	return f.handler.ProcessBlock(blk, obj)
}
