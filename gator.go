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

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"go.uber.org/zap"
)

type Gator interface {
	Pass(block *pbbstream.Block) bool
}

type TimeThresholdGator struct {
	passed    bool
	threshold time.Duration

	logger *zap.Logger
}

func NewTimeThresholdGator(threshold time.Duration, opts ...GateOption) *TimeThresholdGator {
	g := &TimeThresholdGator{
		threshold: threshold,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *TimeThresholdGator) Pass(block *pbbstream.Block) bool {
	if g.passed {
		return true
	}

	blockTime := block.Timestamp.AsTime()
	g.passed = time.Since(blockTime) < g.threshold
	if g.passed {
		g.logger.Info("gator passed on blocktime")
	}
	return g.passed
}

func (g *TimeThresholdGator) SetLogger(logger *zap.Logger) {
	g.logger = logger
}

type BlockNumberGator struct {
	passed    bool
	blockNum  uint64
	exclusive bool

	logger *zap.Logger
}

func NewBlockNumberGator(blockNum uint64, opts ...GateOption) *BlockNumberGator {
	g := &BlockNumberGator{
		blockNum: blockNum,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func NewExclusiveBlockNumberGator(blockNum uint64, opts ...GateOption) *BlockNumberGator {
	g := &BlockNumberGator{
		blockNum:  blockNum,
		exclusive: true,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *BlockNumberGator) Pass(block *pbbstream.Block) bool {
	if g.passed {
		return true
	}

	g.passed = block.Number >= g.blockNum
	if g.passed {
		g.logger.Info("gator passed on blocknum", zap.Uint64("block_num", g.blockNum))
		if g.exclusive {
			return false
		}
	}
	return g.passed
}

func (g *BlockNumberGator) SetLogger(logger *zap.Logger) {
	g.logger = logger
}
