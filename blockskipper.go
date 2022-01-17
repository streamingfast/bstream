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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/dstore"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	"go.uber.org/zap"
)

type BlockSkipper interface {

	// Should we skip that block
	Skip(BlockRef) bool

	// These things are ideas for when we have indexes with partial segments

	//// Process will give you an array of blocks to process, given a single block.
	//// 1. It can return empty array if the block can be skipped
	//// 2. It can return the same block that was given in input if it must be processed
	//// 3. It can return a few more blocks that were sent out-of-order before
	//// It may add a flag to the block  (ex: KnownAsIrreversible)
	//NextBlock(*Block) []*Block

	//// NextRange will give you the next 100-blocks-file that you should read, given a start_block
	//NextRange(uint64) uint64
}

func newForkedBlocksSkipper(store dstore.Store, bundleSizes []uint64, reqBlockMatch BlockRef) *forkedBlocksSkipper {

	//FIXME not sure this belongs here... we want bundleSizes always bigger first
	sort.Slice(bundleSizes, func(i, j int) bool { return bundleSizes[i] > bundleSizes[j] })

	return &forkedBlocksSkipper{
		store:         store,
		bundleSizes:   bundleSizes,
		reqBlockMatch: reqBlockMatch,
	}

}

type forkedBlocksSkipper struct {
	passThrough bool // never skip, never load anything

	loadedUpperBoundary uint64
	reqBlockMatch       BlockRef

	nextBlockIDs []string
	store        dstore.Store
	bundleSizes  []uint64
}

func (s *forkedBlocksSkipper) Skip(blk BlockRef) bool {
	if s.passThrough {
		return false
	}
	if blk.Num() > s.loadedUpperBoundary {
		if !s.loadRange(blk.Num()) {
			return false
		}
		if s.reqBlockMatch != nil {
			fmt.Println("reqblockmatch is nil?", s.reqBlockMatch == nil)
			if s.reqBlockMatch.ID() != "" {
				var foundMatching bool
				for _, b := range s.nextBlockIDs {
					if b == s.reqBlockMatch.ID() {
						foundMatching = true
					}
				}
				if !foundMatching {
					s.passThrough = true
					return false
				}
				s.reqBlockMatch = nil // no more need for that
			}
		}
	}

	for i, b := range s.nextBlockIDs {
		if blk.ID() == b {
			s.nextBlockIDs = removeIndex(s.nextBlockIDs, i)
			return false
		}
	}
	return true

}

func removeIndex(s []string, index int) []string {
	ret := make([]string, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

func (s *forkedBlocksSkipper) loadRange(blockNum uint64) (found bool) {
	for _, size := range s.bundleSizes {
		baseBlockNum := blockNum - (blockNum % size)
		irrBlockIDs, err := getIrreversibleIndex(baseBlockNum, s.store, size)
		if err != nil {
			zlog.Warn("error fetching irreversible index",
				zap.Uint64("base_block_num", baseBlockNum),
				zap.Error(err),
			)
			continue
		}
		if irrBlockIDs != nil {
			for _, b := range irrBlockIDs {
				s.nextBlockIDs = append(s.nextBlockIDs, b)
			}
			s.loadedUpperBoundary = baseBlockNum + size - 1
			return true
		}
	}

	zlog.Debug("no irreversible index at this height, disabling lookups for next blocks", zap.Uint64("block_num", blockNum))
	s.passThrough = true
	return false
}

//func NextRange(in uint64) uint64 {
//	// can this break filesource for big holes ???
//	return in // never skip any 100-blocks-file for irreversibility
//}

//func NextBlock(in uint64) uint64 {
//	// can this break filesource ?
//	return in // never skip any 100-blocks-file for irreversibility
//}

func getIrreversibleIndex(baseBlockNum uint64, store dstore.Store, bundleSize uint64) ([]string, error) {
	filename := fmt.Sprintf("%010d.%d.irr.idx", baseBlockNum, bundleSize)
	reader, err := store.OpenObject(context.Background(), filename)
	if err != nil {
		if errors.Is(dstore.ErrNotFound, err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cannot fetch %s from irreversible blocks index store: %w", filename, err)
	}

	bts, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s from irreversible blocks index store: %w", filename, err)
	}

	resp := &pbblockmeta.BlockRefs{}
	err = proto.Unmarshal(bts, resp)
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal proto of %s: %w", filename, err)
	}

	out := []string{} //empty array here is not the same as nil
	for _, b := range resp.BlockRefs {
		out = append(out, b.BlockID)
	}

	return out, nil
}
