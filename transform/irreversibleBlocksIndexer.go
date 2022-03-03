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

package transform

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var IndexWriteTimeout = 15 * time.Second
var CleanupPeriod = 5 * time.Second

// RecentBlockGetter requires a source that shuts down when ProcessBlock fails
type IrreversibleBlocksIndexer struct {
	store             dstore.Store
	baseBlockNums     map[uint64]uint64
	blocks            []bstream.BlockRef
	firstBlockSeen    uint64
	lastCleanup       time.Time
	definedStartBlock *uint64
}

func NewIrreversibleBlocksIndexer(store dstore.Store, bundleSizes []uint64, opts ...IrreversibleIndexerOption) *IrreversibleBlocksIndexer {
	i := &IrreversibleBlocksIndexer{
		store:         store,
		baseBlockNums: toMap(bundleSizes),
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type IrreversibleIndexerOption func(*IrreversibleBlocksIndexer)

func IrrWithDefinedStartBlock(startBlock uint64) IrreversibleIndexerOption {
	return func(i *IrreversibleBlocksIndexer) {
		i.definedStartBlock = &startBlock
	}
}

func (n *IrreversibleBlocksIndexer) initializeFromFirstBlock(blk bstream.BlockRef) {
	blockNum := blk.Num()
	n.firstBlockSeen = blockNum

	if blockNum == bstream.GetProtocolFirstStreamableBlock {
		for i := range n.baseBlockNums {
			// all low boundaries fit with first streamable block
			n.baseBlockNums[i] = lowBoundary(blockNum, i)
		}
		return
	}

	if n.definedStartBlock != nil {
		for i := range n.baseBlockNums {
			// ex: block:102 on index_size:100 matches definedStartBlock:100
			//     block:102 on index_size:1000 matches definedStartBlock:0
			//     block 1002 matches definedStartBlock:1000 on both index sizes
			if lowBoundary(blockNum, i) >= *n.definedStartBlock {
				n.baseBlockNums[i] = *n.definedStartBlock
			}
		}
		n.firstBlockSeen = *n.definedStartBlock
		return
	}
}

func (n *IrreversibleBlocksIndexer) Add(blk bstream.BlockRef) {
	n.blocks = append(n.blocks, blk)

	if n.firstBlockSeen == 0 {
		n.initializeFromFirstBlock(blk)
	}
	for indexSize, baseBlockNum := range n.baseBlockNums {
		passedBoundary := blk.Num() >= indexSize+baseBlockNum
		if passedBoundary {
			bundleComplete := n.firstBlockSeen <= baseBlockNum || n.firstBlockSeen == bstream.GetProtocolFirstStreamableBlock
			if bundleComplete {
				var refs []bstream.BlockRef
				for _, b := range n.blocks {
					if b.Num() >= baseBlockNum && b.Num() < baseBlockNum+indexSize {
						refs = append(refs, b)
					}
				}
				if len(refs) > 0 {
					n.writeIndex(toFilename(indexSize, baseBlockNum), refs)
				}
			}
			n.baseBlockNums[indexSize] = lowBoundary(blk.Num(), indexSize)
		}
	}
	if time.Since(n.lastCleanup) > CleanupPeriod {
		n.cleanUp()
	}
}

func (n *IrreversibleBlocksIndexer) cleanUp() {
	var lowest uint64
	for _, i := range n.baseBlockNums {
		if lowest == 0 {
			lowest = i
			continue
		}
		if i < lowest {
			lowest = i
		}
	}

	var newArray []bstream.BlockRef

	for _, blk := range n.blocks {
		if blk.Num() >= lowest {
			newArray = append(newArray, blk)
		}
	}
	n.blocks = newArray

	n.lastCleanup = time.Now()
}

func (n *IrreversibleBlocksIndexer) writeIndex(filename string, blocks []bstream.BlockRef) {
	ctx, cancel := context.WithTimeout(context.Background(), IndexWriteTimeout)
	defer cancel()

	var blockrefs []*pbblockmeta.BlockRef
	for _, b := range blocks {
		blockrefs = append(blockrefs, &pbblockmeta.BlockRef{
			BlockNum: b.Num(),
			BlockID:  b.ID(),
		})
	}

	data, err := proto.Marshal(&pbblockmeta.BlockRefs{
		BlockRefs: blockrefs,
	})
	if err != nil {
		zlog.Warn("cannot write marshal data from blockref", zap.Error(err))
		return
	}

	if err := n.store.WriteObject(ctx, filename, bytes.NewReader(data)); err != nil {
		zlog.Warn("cannot write index file to store",
			zap.String("filename", filename),
			zap.Error(err),
		)
	}
}

func toMap(bundleSizes []uint64) map[uint64]uint64 {
	out := map[uint64]uint64{}
	for _, bs := range bundleSizes {
		out[bs] = 0
	}
	return out
}

func toFilename(bundleSize, baseBlockNum uint64) string {
	return fmt.Sprintf("%010d.%d.irr.idx", baseBlockNum, bundleSize)
}
