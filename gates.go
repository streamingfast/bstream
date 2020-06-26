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

	"go.uber.org/zap"
)

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
	Name string

	blockNum uint64
	handler  Handler
	gateType GateType

	MaxHoldOff      int
	maxHoldOffCount int

	passed bool
}

func NewBlockNumGate(blockNum uint64, gateType GateType, h Handler) *BlockNumGate {
	return &BlockNumGate{
		blockNum:   blockNum,
		gateType:   gateType,
		handler:    h,
		MaxHoldOff: 15000,
	}
}

func (g *BlockNumGate) ProcessBlock(blk *Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	g.passed = blk.Num() >= g.blockNum

	if (g.blockNum == 0 || g.blockNum == 1) && blk.Num() == 2 {
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

	zlog.Info("block num gate passed", zap.String("gate_type", g.gateType.String()), zap.Uint64("at_block_num", blk.Num()), zap.Uint64("gate_block_num", g.blockNum))

	if g.gateType == GateInclusive {
		return g.handler.ProcessBlock(blk, obj)
	}
	return nil
}

type BlockIDGate struct {
	Name string

	blockID  string
	handler  Handler
	gateType GateType

	MaxHoldOff      int
	maxHoldOffCount int

	passed bool
}

func NewBlockIDGate(blockID string, gateType GateType, h Handler) *BlockIDGate {
	return &BlockIDGate{
		blockID:    blockID,
		gateType:   gateType,
		handler:    h,
		MaxHoldOff: 15000,
	}
}

func (g *BlockIDGate) ProcessBlock(blk *Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	g.passed = blk.ID() == g.blockID

	if (g.blockID == "" || g.blockID == "0000000000000000000000000000000000000000000000000000000000000000") && blk.Num() == 2 {
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

	zlog.Info("block id gate passed", zap.String("gate_type", g.gateType.String()), zap.String("block_id", g.blockID))

	if g.gateType == GateInclusive {
		return g.handler.ProcessBlock(blk, obj)
	}
	return nil
}

///////////////////////////////////////

type RealtimeGate struct {
	Name string

	timeToRealtime time.Duration
	handler        Handler
	gateType       GateType

	passed bool
}

func NewRealtimeGate(timeToRealtime time.Duration, h Handler) *RealtimeGate {
	return &RealtimeGate{
		timeToRealtime: timeToRealtime,
		handler:        h,
	}
}

func (g *RealtimeGate) ProcessBlock(blk *Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	blockTime := blk.Time()
	delta := time.Since(blockTime)
	g.passed = delta < g.timeToRealtime
	if !g.passed {
		return nil
	}

	zlog.Info("realtime gate passed", zap.String("name", g.Name), zap.Duration("delta", delta))

	return g.handler.ProcessBlock(blk, obj)
}

func (g *RealtimeGate) SetName(name string) {
	g.Name = name
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
}

func NewRealtimeTripper(timeToRealtime time.Duration, tripFunc func(), h Handler) *RealtimeTripper {
	return &RealtimeTripper{
		timeToRealtime: timeToRealtime,
		tripFunc:       tripFunc,
		handler:        h,
		nowFunc:        time.Now,
	}
}

func (t *RealtimeTripper) ProcessBlock(blk *Block, obj interface{}) error {
	if t.passed {
		return t.handler.ProcessBlock(blk, obj)
	}

	now := t.nowFunc()
	blockTime := blk.Time()
	delta := now.Sub(blockTime)

	t.passed = delta < t.timeToRealtime
	if t.passed {
		t.tripFunc()
		zlog.Info("realtime tripper tripped", zap.Duration("delta", delta))
	}

	// This works well for EOS and ETH, we simply want to print the advancement when more from live source than batch of blocks.
	// Hence, if last time we seen a block, more than 0.45 elapsed, it's probably a live block.
	if !t.passed && time.Since(t.lastBlockSeenAt).Seconds() > 0.45 {
		zlog.Info("realtime tripper seen block but still not realtime according to tolerance, waiting for realtime block to appear", zap.Stringer("block", blk), zap.Duration("delta", delta), zap.Duration("realtime_tolerance", t.timeToRealtime))
	}

	t.lastBlockSeenAt = now
	return t.handler.ProcessBlock(blk, obj)
}
