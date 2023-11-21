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

package forkable

import (
	"fmt"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

// IrreversibleBlockNumGate This gate lets all blocks through once the target blocknum has passed AS IRREVERSIBLE
type IrreversibleBlockNumGate struct {
	blockNum uint64
	handler  bstream.Handler
	gateType bstream.GateType

	MaxHoldOff      int
	maxHoldOffCount int

	passed bool

	logger *zap.Logger
}

func NewIrreversibleBlockNumGate(blockNum uint64, gateType bstream.GateType, h bstream.Handler, opts ...bstream.GateOption) *IrreversibleBlockNumGate {
	g := &IrreversibleBlockNumGate{
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

func (g *IrreversibleBlockNumGate) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	fobj := obj.(*ForkableObject)
	if fobj.step != bstream.StepIrreversible {
		return nil
	}

	g.passed = blk.Number >= g.blockNum

	if (g.blockNum == 0 || g.blockNum == 1) && blk.Number == 2 {
		g.gateType = bstream.GateInclusive
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

	g.logger.Info("irreversible block num gate passed", zap.String("gate_type", g.gateType.String()), zap.Uint64("at_block_num", blk.Number), zap.Uint64("gate_block_num", g.blockNum))

	if g.gateType == bstream.GateInclusive {
		return g.handler.ProcessBlock(blk, obj)
	}
	return nil
}

func (g *IrreversibleBlockNumGate) SetLogger(logger *zap.Logger) {
	g.logger = logger
}

// This gate lets all blocks through once the target block ID has passed AS IRREVERSIBLE
type IrreversibleBlockIDGate struct {
	blockID  string
	handler  bstream.Handler
	gateType bstream.GateType

	MaxHoldOff      int
	maxHoldOffCount int

	passed bool

	logger *zap.Logger
}

func NewIrreversibleBlockIDGate(blockID string, gateType bstream.GateType, h bstream.Handler, opts ...bstream.GateOption) *IrreversibleBlockIDGate {
	g := &IrreversibleBlockIDGate{
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

func (g *IrreversibleBlockIDGate) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	if g.passed {
		return g.handler.ProcessBlock(blk, obj)
	}

	g.passed = blk.Id == g.blockID

	if (g.blockID == "" || g.blockID == "0000000000000000000000000000000000000000000000000000000000000000") && blk.Number == 2 {
		g.gateType = bstream.GateInclusive
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

	if g.gateType == bstream.GateInclusive {
		return g.handler.ProcessBlock(blk, obj)
	}
	return nil
}

func (g *IrreversibleBlockIDGate) SetLogger(logger *zap.Logger) {
	g.logger = logger
}
