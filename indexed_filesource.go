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
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type BlockIndex interface {
	// Skip() informs a preprocessor about blocks that can be skipped completely
	Skip(BlockRef) bool

	// NextRange() informs about the next block files that should be read
	//
	// It is NOT threadsafe
	// Given the last block that was accepted (or nil), will inform you
	// of the next lowest block number that you need to load
	// If the next range does NOT have an index, it also gives you the previous linkable
	// block reference to set as LIB in your forkable
	NextRange(lastProcessed BlockRef) (baseNum uint64, lib BlockRef, hasIndex bool)

	// Send a blocks in here and receive it back, or nothing, or the last few unsent blocks, reordered
	// ex: Reorder(blk3) -> []
	//     Reorder(blk2) -> []
	//     Reorder(blk1) -> [blk1, blk2, blk3]
	Reorder(blk *PreprocessedBlock) []*PreprocessedBlock
}

type IndexedFileSource struct {
	shutter.Shutter

	handler    Handler
	blockIndex BlockIndex
	blockStore dstore.Store

	nextSourceFactory func(startBlockNum uint64, h Handler, lastFromIndex BlockRef) Source // like SourceFromNum but with the last Block ... or maybe I could give a cursor ????? ohhhh SourceFromCursorFactory?

	preprocFunc PreprocessFunc
}

func (s *IndexedFileSource) Run() {

}

// returns nil if requiredBlock isn't there or if no index exists at startBlockNum. ... oh Maybe I get a cursor here too
func newIrreversibleBlocksIndex(store dstore.Store, bundleSizes []uint64, startBlockNum, uint64, requiredBlock BlockRef) *irrBlocksIndex {

	//FIXME not sure this belongs here... we want bundleSizes always bigger first
	sort.Slice(bundleSizes, func(i, j int) bool { return bundleSizes[i] > bundleSizes[j] })

	return &irrBlocksIndex{
		store:         store,
		bundleSizes:   bundleSizes,
		reqBlockMatch: requiredBlock,
	}

	//FIXME check the existence of requiredBlock and return an error ?

}

type irrBlocksIndex struct {
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
