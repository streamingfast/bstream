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
	"sort"
	"sync"

	"go.uber.org/zap"
)

type TailLockOption = func(s *TailLock)

func TailLockWithLogger(logger *zap.Logger) TailLockOption {
	return func(s *TailLock) {
		s.logger = logger
	}
}

// TailLock manages inflight block queries, to feed into
// truncation mechanism, so it happens only when buffer is full, and
// no one is querying the blocks.
type TailLock struct {
	sync.Mutex
	inflights map[uint64]int

	logger *zap.Logger
}

func NewTailLock(opts ...TailLockOption) *TailLock {
	g := &TailLock{
		inflights: make(map[uint64]int),
		logger:    zlog,
	}

	for _, opt := range opts {
		opt(g)
	}

	return g
}

func (g *TailLock) TailLock(blockNum uint64) (releaseFunc func()) {
	g.Lock()
	defer g.Unlock()

	g.logger.Debug("adding lower bound guard", zap.Uint64("block_num", blockNum))

	g.add(blockNum)

	var do sync.Once // all callers were doing this, let's bake it into the releaseFunc
	return func() {
		do.Do(func() {
			g.remove(blockNum)
		})
	}
}

func (g *TailLock) remove(blockNum uint64) {
	g.Lock()
	defer g.Unlock()

	if x, ok := g.inflights[blockNum]; ok {
		if x == 1 {
			delete(g.inflights, blockNum)
			return
		}
		g.inflights[blockNum] = x - 1
	} else {
		g.logger.Fatal("error on promises handling in buffer")
	}
}

func (g *TailLock) add(blockNum uint64) {
	if x, ok := g.inflights[blockNum]; ok {
		g.inflights[blockNum] = x + 1
	} else {
		g.inflights[blockNum] = 1
	}
}

func (g *TailLock) LowerBound() uint64 {
	g.Lock()
	defer g.Unlock()

	if len(g.inflights) == 0 {
		return 0
	}

	blockNums := make([]uint64, len(g.inflights))

	i := 0
	for bnum := range g.inflights {
		blockNums[i] = bnum
		i++
	}

	sort.Slice(blockNums, func(i, j int) bool { return blockNums[i] < blockNums[j] })

	return blockNums[0]
}
