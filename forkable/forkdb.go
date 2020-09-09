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

	"github.com/dfuse-io/bstream"
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
	// nums contain block_id -> block_num
	nums map[string]uint64

	// objects contain objects of whatever nature you want to associate with blocks (lists of transaction IDs, Block, etc..
	objects map[string]interface{}

	libID  string
	libNum uint64

	logger *zap.Logger
}

func NewForkDB(opts ...ForkDBOption) *ForkDB {
	db := &ForkDB{
		links:   make(map[string]string),
		nums:    make(map[string]uint64),
		objects: make(map[string]interface{}),
		logger:  zlog,
	}

	for _, opt := range opts {
		opt(db)
	}

	return db
}

func (f *ForkDB) InitLIB(ref bstream.BlockRef) {
	f.libID = ref.ID()
	f.libNum = ref.Num()
}

func (f *ForkDB) HasLIB() bool {
	return f.libID != ""
}

func (f *ForkDB) SetLogger(logger *zap.Logger) {
	f.logger = logger
}

// TrySetLIB will move the lib if crawling from the given blockID up to the dposlibNum
// succeeds, giving us effectively the dposLIBID. It will perform the set LIB and set
// the new headBlockID
// unknown behaviour if it was already set ... maybe it explodes
func (f *ForkDB) TrySetLIB(headRef, previousRef bstream.BlockRef, libNum uint64) {
	if headRef.Num() == bstream.GetProtocolFirstStreamableBlock {
		f.libID = previousRef.ID()
		f.libNum = bstream.GetProtocolGenesisBlock
		libNum = bstream.GetProtocolGenesisBlock

		f.logger.Debug("candidate LIB received is first streamable block of chain, assuming it's the new LIB", zap.Stringer("lib", bstream.NewBlockRef(f.libID, f.libNum)))
	}
	libRef := f.BlockInCurrentChain(headRef, libNum)
	if libRef.ID() == "" {
		f.logger.Debug("missing links to back fill cache to LIB num", zap.String("head_id", headRef.ID()), zap.Uint64("head_num", headRef.Num()), zap.Uint64("previous_ref_num", headRef.Num()), zap.Uint64("lib_num", libNum), zap.Uint64("get_protocol_first_block", bstream.GetProtocolFirstStreamableBlock))
		return
	}

	_ = f.MoveLIB(libRef)
}

// Get the last irreversible block ID
func (f *ForkDB) LIBID() string {
	return f.libID
}

// Get the last irreversible block num
func (f *ForkDB) LIBNum() uint64 {
	return f.libNum
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
	if !f.HasLIB() {
		return
	}

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

	return f.links[blockID] != ""
}

func (f *ForkDB) AddLink(blockRef, previousRef bstream.BlockRef, obj interface{}) (exists bool) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	blockID := blockRef.ID()
	if f.links[blockID] != "" {
		return true
	}

	previousID := previousRef.ID()

	f.links[blockID] = previousID
	f.nums[blockID] = blockRef.Num()
	f.nums[previousID] = previousRef.Num()

	if obj != nil {
		f.objects[blockID] = obj
	}

	return false
}

// BlockInCurrentChain finds the block_id at height `blockNum` under
// the requested `startAtBlockID` base block. Passing the head block id
// as `startAtBlockID` will tell you if the block num is part of the longuest
// chain.
func (f *ForkDB) BlockInCurrentChain(startAtBlock bstream.BlockRef, blockNum uint64) bstream.BlockRef {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	cur := startAtBlock.ID()
	for {
		prev := f.links[cur]
		if prev == "" {
			return bstream.BlockRefEmpty
		}

		prevNum := f.nums[prev]
		if prevNum == blockNum {
			return bstream.NewBlockRef(prev, prevNum)
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
func (f *ForkDB) ReversibleSegment(upToBlock bstream.BlockRef) (blocks []*Block) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	if !f.HasLIB() {
		panic("the LIB ID is not defined and should have been")
	}

	var reversedBlocks []*Block

	cur := upToBlock.ID()
	curNum := upToBlock.Num()

	for {
		if curNum > bstream.GetProtocolFirstStreamableBlock && curNum < f.LIBNum() {
			f.logger.Debug("forkdb linking past known irreversible block", zap.Uint64("lib_num", f.LIBNum()), zap.String("lib", f.libID), zap.String("block_id", cur), zap.Uint64("block_num", curNum))
			return nil
		}

		if cur == f.libID {
			break
		}

		reversedBlocks = append(reversedBlocks, &Block{
			BlockID:  cur,
			BlockNum: curNum,
			Object:   f.objects[cur],
		})

		prev := f.links[cur]
		if prev == "" {
			f.logger.Debug("forkdb unlinkable block", zap.String("block_id", cur), zap.Uint64("block_num", curNum))
			return nil
		}

		cur = prev
		curNum = f.nums[prev]
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
	if f.libID == "" || len(blocks) == 0 {
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

// HasNewIrreversibleSegment returns segments upon psasing the
// newDposLIBID that are irreversible and stale. If there was no new
// segment, `hasNew` will be false. WARN: this method can only be
// called when `HasLIB()` is true.  Otherwise, it panics.
func (f *ForkDB) HasNewIrreversibleSegment(newLIB bstream.BlockRef) (hasNew bool, irreversibleSegment, staleBlocks []*Block) {
	newLIBID := newLIB.ID()
	if f.libID == newLIBID {
		return false, nil, nil
	}

	irreversibleSegment = f.ReversibleSegment(newLIB)
	if len(irreversibleSegment) == 0 {
		return false, nil, nil
	}

	staleBlocks = f.stalledInSegment(irreversibleSegment)

	return true, irreversibleSegment, staleBlocks
}

func (f *ForkDB) MoveLIB(blockRef bstream.BlockRef) (purgedBlocks []*Block) {
	f.linksLock.Lock()
	defer f.linksLock.Unlock()

	blockNum := blockRef.Num()

	newLinks := make(map[string]string)
	newNums := make(map[string]uint64)

	for from, to := range f.links {
		fromNum := f.nums[from]

		if fromNum >= blockNum {
			newLinks[from] = to
			newNums[from] = fromNum
			newNums[to] = f.nums[to]
		} else {
			// FIXME: this isn't read by anyone.. continue creating it?
			purgedBlocks = append(purgedBlocks, &Block{
				BlockID:         from,
				BlockNum:        fromNum,
				Object:          f.objects[from],
				PreviousBlockID: to,
			})

			delete(f.objects, from)
		}
	}

	f.links = newLinks
	f.nums = newNums
	f.libID = blockRef.ID()
	f.libNum = blockNum

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
