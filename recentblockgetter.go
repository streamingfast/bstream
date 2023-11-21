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
	"sync"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
)

// RecentBlockGetter requires a source that shuts down when ProcessBlock fails
type RecentBlockGetter struct {
	counter         int
	mostRecentBlock *pbbstream.Block
	lock            sync.Mutex
	sampleSize      int
}

func NewRecentBlockGetter(sampleSize int) *RecentBlockGetter {
	return &RecentBlockGetter{
		sampleSize: sampleSize,
	}
}

func (g *RecentBlockGetter) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	g.lock.Lock()
	defer g.lock.Unlock()

	if g.mostRecentBlock == nil || blk.Time().After(g.mostRecentBlock.Time()) {
		g.mostRecentBlock = blk
	}

	g.counter++
	if g.counter >= g.sampleSize {
		return fmt.Errorf("done")
	}

	return nil
}

func (g *RecentBlockGetter) LatestBlock() *pbbstream.Block {
	g.lock.Lock()
	defer g.lock.Unlock()

	return g.mostRecentBlock
}
