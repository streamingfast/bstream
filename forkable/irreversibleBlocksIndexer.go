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
	store          dstore.Store
	baseBlockNums  map[uint64]uint64
	blocks         []bstream.BlockRef
	handler        bstream.Handler
	firstBlockSeen uint64
	lastCleanup    time.Time
}

func NewIrreversibleBlocksIndexer(store dstore.Store, bundleSizes []uint64, h bstream.Handler) *IrreversibleBlocksIndexer {
	return &IrreversibleBlocksIndexer{
		store:         store,
		baseBlockNums: toMap(bundleSizes),
		handler:       h,
	}
}

func (n *IrreversibleBlocksIndexer) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	fobj := obj.(*ForkableObject)

	if fobj.step == bstream.StepIrreversible {
		n.add(blk.AsRef())
	}
	return n.handler.ProcessBlock(blk, obj)

}

func (n *IrreversibleBlocksIndexer) add(blk bstream.BlockRef) {
	n.blocks = append(n.blocks, blk)

	if n.firstBlockSeen == 0 {
		n.firstBlockSeen = blk.Num()
	}
	// i is size of bundles (ex: 100, 1000)
	// j is base blocknum of current bundle of each size
	for i, j := range n.baseBlockNums {
		if blk.Num()-j >= i { // passed boundary
			var refs []bstream.BlockRef
			for _, b := range n.blocks {
				if b.Num() >= j && b.Num() < j+i {
					refs = append(refs, b)
				}
			}
			if len(refs) > 0 && (n.firstBlockSeen <= j || n.firstBlockSeen == bstream.GetProtocolFirstStreamableBlock) {
				n.writeIndex(toFilename(i, j), refs)
			}
			n.baseBlockNums[i] = blk.Num() - (blk.Num() % i)
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
