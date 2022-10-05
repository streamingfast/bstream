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
	"sort"
	"sync"

	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

type ForkDBOption func(db *ForkDB)

func ForkDBWithLogger(logger *zap.Logger) ForkDBOption {
	return func(db *ForkDB) {
		db.logger = logger
	}
}

// ForkDB holds the graph of block headBlockID to previous block.
type ForkDB struct {
	// links contain block_id -> previous_block_id
	links     map[string]string
	linksLock sync.Mutex
	// nums contain block_id -> block_num. For blocks that were not EXPLICITLY added through AddLink (as the first BlockRef) or added through InitLIB(), the number will not be set. A missing reference means this is a block ID pointing to a non-LIB, yet Root block that we have obtains only through it being referenced as a PreviousID in an AddBlock call.
	nums map[string]uint64

	// objects contain objects of whatever nature you want to associate with blocks (lists of transaction IDs, Block, etc..
	objects map[string]interface{}

	libRef bstream.BlockRef

	logger *zap.Logger
}

func NewForkDB(opts ...ForkDBOption) *ForkDB {
	db := &ForkDB{
		links:   make(map[string]string),
		nums:    make(map[string]uint64),
		objects: make(map[string]interface{}),
		libRef:  bstream.BlockRefEmpty,
		logger:  zlog,
	}

	for _, opt := range opts {
		opt(db)
	}

	return db
}

func (f *ForkDB) InitLIB(ref bstream.BlockRef) {
	f.libRef = ref
	f.nums[ref.ID()] = ref.Num()
}

func (f *ForkDB) HasLIB() bool {
	if f.libRef == nil {
		return false
	}

	return !bstream.EqualsBlockRefs(f.libRef, bstream.BlockRefEmpty)
}

func (f *ForkDB) SetLogger(logger *zap.Logger) {
	f.logger = logger
}

// TrySetLIB will move the lib if crawling from the given blockID up to the dposlibNum
// succeeds, giving us effectively the dposLIBID. It will perform the set LIB and set
// the new headBlockID
// unknown behaviour if it was already set ... maybe it explodes
func (f *ForkDB) TrySetLIB(headRef bstream.BlockRef, previousRefID string, libNum uint64) {
	if headRef.Num() == bstream.GetProtocolFirstStreamableBlock {
		f.libRef = headRef
		f.logger.Debug("TrySetLIB received first streamable block of chain, assuming it's the new LIB", zap.Stringer("lib", f.libRef))
		return
	}
	libRef := f.BlockInCurrentChain(headRef, libNum)
	if libRef.ID() == "" {
		f.logger.Debug("missing links to back fill cache to LIB num", zap.String("head_id", headRef.ID()), zap.Uint64("head_num", headRef.Num()), zap.Uint64("previous_ref_num", headRef.Num()), zap.Uint64("lib_num", libNum), zap.Uint64("get_protocol_first_block", bstream.GetProtocolFirstStreamableBlock))
		return
	}

	_ = f.MoveLIB(libRef)
}

//Set a new lib without cleaning up blocks older then new lib (NO MOVE)
func (f *ForkDB) SetLIB(headRef bstream.BlockRef, previousRefID string, libNum uint64) {
	if headRef.Num() == bstream.GetProtocolFirstStreamableBlock {
		f.libRef = headRef
		f.logger.Debug("SetLIB received first streamable block of chain, assuming it's the new LIB", zap.Stringer("lib", f.libRef))
		return
	}
	libRef := f.BlockInCurrentChain(headRef, libNum)
	if libRef.ID() == "" {
		f.logger.Debug("missing links to back fill cache to LIB num", zap.String("head_id", headRef.ID()), zap.Uint64("head_num", headRef.Num()), zap.Uint64("previous_ref_num", headRef.Num()), zap.Uint64("lib_num", libNum), zap.Uint64("get_protocol_first_block", bstream.GetProtocolFirstStreamableBlock))
		return
	}

	f.libRef = libRef
}

// Get the last irreversible block ID
func (f *ForkDB) LIBID() string {
	return f.libRef.ID()
}

// Get the last irreversible block num
func (f *ForkDB) LIBNum() uint64 {
	return f.libRef.Num()
}

func (f *ForkDB) IsBehindLIB(blockNum uint64) bool {
	return blockNum <= f.LIBNum()
}

// ChainSwitchSegments returns the list of block IDs that should be
// `undo`ne (in reverse chain order) and the list of blocks that
// should be `redo`ne (in chain order) for `blockID` (linking to
// `previousID`) to become the longest chain.
//
// This assumes you are querying for something that *is* the longest
// chain (or the to-become longest chain).
func (f *ForkDB) ChainSwitchSegments(oldHeadBlockID, newHeadsPreviousID string) (undo []string, redo []string) {
	cur := oldHeadBlockID
	var undoChain []string
	seen := make(map[string]struct{})

	f.linksLock.Lock()
	for {
		undoChain = append(undoChain, cur)
		seen[cur] = struct{}{}

		prev := f.links[cur]
		if prev == "" {
			break
		}
		cur = prev
	}
	f.linksLock.Unlock()

	cur = newHeadsPreviousID
	var redoChain []string
	var junctionBlock string
	for {
		if _, found := seen[cur]; found {
			junctionBlock = cur
			break
		}
		redoChain = append(redoChain, cur)

		prev := f.links[cur]
		if prev == "" {
			// couldn't reach a common point, probably unlinked
			return nil, nil
		}
		cur = prev
	}

	var truncatedUndo []string
	for _, blk := range undoChain {
		if blk == junctionBlock {
			break
		}
		truncatedUndo = append(truncatedUndo, blk)
	}

	// WARN: what happens if `junctionBlock` isn't found?
	// This should not happen if we DO have links up until LIB.

	l := len(redoChain)
	var reversedRedo []string
	for i := 0; i < l; i++ {
		reversedRedo = append(reversedRedo, redoChain[l-i-1])
	}

	return truncatedUndo, reversedRedo
}

func (f *ForkDB) Exists(blockID string) bool {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	_, ok := f.links[blockID]
	return ok
}

func (f *ForkDB) AddLink(blockRef bstream.BlockRef, previousRefID string, obj interface{}) (exists bool) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	blockID := blockRef.ID()
	if blockID == previousRefID || blockID == "" {
		return false
	}

	if f.links[blockID] != "" {
		return true
	}

	f.links[blockID] = previousRefID
	f.nums[blockID] = blockRef.Num()
	// MEANS f.nums will NOT provide the blockNumber associated with a block that was
	// not EXPLICITLY added as a blockRef (not a previous reference)
	//f.nums[previousID] = previousRef.Num()

	if obj != nil {
		f.objects[blockID] = obj
	}

	return false
}

// BlockInCurrentChain finds the block_id at height `blockNum` under
// the requested `startAtBlockID` base block. Passing the head block id
// as `startAtBlockID` will tell you if the block num is part of the longest
// chain.
func (f *ForkDB) BlockInCurrentChain(startAtBlock bstream.BlockRef, blockNum uint64) bstream.BlockRef {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()
	if startAtBlock.Num() == blockNum {
		return startAtBlock
	}

	cur := startAtBlock.ID()
	for {
		prev := f.links[cur]
		prevNum, found := f.nums[prev]
		if !found {
			// This means it is a ROOT block, or you're in the middle of a HOLE
			zlog.Debug("found root or hole. did not reach requested block", zap.Uint64("requested_block_num", blockNum), zap.String("missing_id", prev))
			return bstream.BlockRefEmpty
		}

		if prevNum == blockNum {
			return bstream.NewBlockRef(prev, prevNum)
		} else if prevNum < blockNum {
			// in case blockNum is 500 and the prev is 499, whereas previous check had prev == 501
			// meaning there would be a hole in contiguity of the block numbers
			// on chains where this is possible.
			return bstream.NewBlockRef(cur, blockNum)
		}

		cur = prev
	}
}

// ReversibleSegment returns the blocks between the previous
// irreversible Block ID and the given block ID.  The LIB is
// excluded and the given block ID is included in the results.
//
// Do not call this function is the .HasLIB() is false, as the result
// would make no sense.
//
// WARN: if the segment is broken by some unlinkable blocks, the
// return value is `nil`.
func (f *ForkDB) ReversibleSegment(startBlock bstream.BlockRef) (blocks []*Block, reachLIB bool) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	var reversedBlocks []*Block

	curID := startBlock.ID()
	curNum := startBlock.Num()

	// Those are for debugging purposes, they are the value of `curID` and `curNum`
	// just before those are switched to a previous parent link,
	prevID := ""
	prevNum := uint64(0)

	seenIDs := make(map[string]bool)
	for {
		if curNum > bstream.GetProtocolFirstStreamableBlock && curNum < f.LIBNum() {
			f.logger.Debug("forkdb linking past known irreversible block",
				zap.Stringer("lib", f.libRef),
				zap.Stringer("start_block", startBlock),
				zap.Stringer("current_block", bstream.NewBlockRef(curID, curNum)),
				zap.Stringer("previous_block", bstream.NewBlockRef(prevID, prevNum)),
			)
			return
		}

		if curID == f.libRef.ID() {
			reachLIB = true
			break
		}

		prev, found := f.links[curID]
		if !found {
			if f.HasLIB() {
				// This was Debug before but when serving Firehose request and there is a hole in one
				// of the merged blocks, it means you see almost nothing since normal logging is at Info.
				// This force usage of debug log to see something. Switched to be a warning since an unlinkable
				// block is not something that should happen, specially between `startBlock` and `LIB`, which is
				// the case here.
				//
				// If you came here to switch to Debug because it's too verbose, we should think about a way to
				// reduce the occurrence, at least logging once at Warn/Info and the rest in Debug.
				f.logger.Warn("forkdb unlinkable block, unable to reach last irrerversible block by following parent links",
					zap.Stringer("lib", f.libRef),
					zap.Stringer("start_block", startBlock),
					zap.String("missing_block_id", curID),
					zap.Stringer("missing_parent_of_block", bstream.NewBlockRef(prevID, prevNum)),
				)
				return nil, false // when LIB is set we need to reach it
			}

			break //reach the root of the chain. This should be the LIB, but we don't know yet.
		}

		reversedBlocks = append(reversedBlocks, &Block{
			BlockID:  curID,
			BlockNum: curNum,
			Object:   f.objects[curID],
		})

		seenIDs[prevID] = true
		prevID = curID
		prevNum = curNum

		curID = prev
		curNum = f.nums[prev]

		if seenIDs[curID] {
			zlog.Error("loop detected in reversible segment", zap.String("cur_id", curID), zap.Uint64("cur_num", curNum))
			return nil, false
		}
	}

	// Reverse sort `blocks`
	blocks = make([]*Block, len(reversedBlocks))
	j := 0
	for i := len(reversedBlocks); i != 0; i-- {
		blocks[j] = reversedBlocks[i-1]
		j++
	}
	return
}

func (f *ForkDB) stalledInSegment(blocks []*Block) (out []*Block) {
	if f.libRef.ID() == "" || len(blocks) == 0 {
		return
	}

	excludeBlocks := make(map[string]bool)
	for _, blk := range blocks {
		excludeBlocks[blk.BlockID] = true
	}

	start := blocks[0].BlockNum
	end := blocks[len(blocks)-1].BlockNum

	f.linksLock.Lock()

	for blkID, prevID := range f.links {
		linkBlkNum := f.nums[blkID]
		if !excludeBlocks[blkID] && linkBlkNum >= start && linkBlkNum <= end {
			out = append(out, &Block{
				BlockID:         blkID,
				BlockNum:        linkBlkNum,
				PreviousBlockID: prevID,
				Object:          f.objects[blkID],
			})
		}
	}
	f.linksLock.Unlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].BlockID < out[j].BlockID
	})

	return out
}

// HasNewIrreversibleSegment returns segments upon passing the
// newDposLIBID that are irreversible and stale. If there was no new
// segment, `hasNew` will be false. WARN: this method can only be
// called when `HasLIB()` is true.  Otherwise, it panics.
func (f *ForkDB) HasNewIrreversibleSegment(newLIB bstream.BlockRef) (hasNew bool, irreversibleSegment, staleBlocks []*Block) {
	if !f.HasLIB() {
		panic("the LIB ID is not defined and should have been")
	}

	newLIBID := newLIB.ID()
	if f.libRef.ID() == newLIBID {
		return false, nil, nil
	}

	irreversibleSegment, _ = f.ReversibleSegment(newLIB)
	if len(irreversibleSegment) == 0 {
		return false, nil, nil
	}

	staleBlocks = f.stalledInSegment(irreversibleSegment)

	return true, irreversibleSegment, staleBlocks
}

func (f *ForkDB) DeleteLink(id string) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()
	delete(f.links, id)
	delete(f.objects, id)
	delete(f.nums, id)
}

func (f *ForkDB) MoveLIB(blockRef bstream.BlockRef) (purgedBlocks []*Block) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	newLib := blockRef.Num()

	newLinks := make(map[string]string)
	newNums := make(map[string]uint64)

	for blk, prev := range f.links {
		blkNum := f.nums[blk]

		if blkNum >= newLib {
			newLinks[blk] = prev
			newNums[blk] = blkNum
		} else {
			purgedBlocks = append(purgedBlocks, &Block{
				BlockID:         blk,
				BlockNum:        blkNum,
				Object:          f.objects[blk],
				PreviousBlockID: prev,
			})

			delete(f.objects, blk)
		}
	}

	f.links = newLinks
	f.nums = newNums
	f.libRef = blockRef

	return
}

// CloneLinks retrieves a snapshot of the links in the ForkDB.  Used
// only in ForkViewerin `eosws`.
func (f *ForkDB) ClonedLinks() (out map[string]string, nums map[string]uint64) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	out = make(map[string]string)
	nums = make(map[string]uint64)

	for k, v := range f.links {
		out[k] = v
		nums[k] = f.nums[k]
	}

	return
}

func (f *ForkDB) BlockForID(blockID string) *Block {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	if previous, ok := f.links[blockID]; ok {
		return &Block{
			BlockID:         blockID,
			BlockNum:        f.nums[blockID],
			PreviousBlockID: previous,
			Object:          f.objects[blockID],
		}
	}

	return nil
}

func (f *ForkDB) IterateLinks(callback func(blockID, previousBlockID string, object interface{}) (getNext bool)) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	for id, prevID := range f.links {
		if !callback(id, prevID, f.objects[id]) {
			break
		}
	}
}
