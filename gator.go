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
	"time"

	"go.uber.org/zap"
)

type Gator interface {
	Pass(block *Block) bool
}

type TimeThresholdGator struct {
	name      string
	passed    bool
	threshold time.Duration
}

func NewTimeThresholdGator(threshold time.Duration) *TimeThresholdGator {
	return &TimeThresholdGator{
		name:      "default",
		threshold: threshold,
	}
}

func (g *TimeThresholdGator) SetName(name string) {
	g.name = name
}

func (g *TimeThresholdGator) Pass(block *Block) bool {
	if g.passed {
		return true
	}

	blockTime := block.Time()
	g.passed = time.Since(blockTime) < g.threshold
	if g.passed {
		zlog.Info("gator passed on blocktime", zap.String("gator_name", g.name))
	}
	return g.passed
}

type BlockNumberGator struct {
	name      string
	passed    bool
	blockNum  uint64
	exclusive bool
}

func (g *BlockNumberGator) SetName(name string) {
	g.name = name
}

func NewBlockNumberGator(blockNum uint64) *BlockNumberGator {
	return &BlockNumberGator{
		name:     "default",
		blockNum: blockNum,
	}
}

func NewExclusiveBlockNumberGator(blockNum uint64) *BlockNumberGator {
	return &BlockNumberGator{
		name:      "default",
		blockNum:  blockNum,
		exclusive: true,
	}
}

func (g *BlockNumberGator) Pass(block *Block) bool {
	if g.passed {
		return true
	}

	g.passed = block.Num() >= g.blockNum
	if g.passed {
		zlog.Info("gator passed on blocknum", zap.String("gator_name", g.name), zap.Uint64("block_num", g.blockNum))
		if g.exclusive {
			return false
		}
	}
	return g.passed
}
