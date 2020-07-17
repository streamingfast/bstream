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
	"fmt"
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
	sync.RWMutex

	chain  *chain
	libRef bstream.BlockRef

	logger *zap.Logger
}

func NewForkDB(opts ...ForkDBOption) *ForkDB {
	db := &ForkDB{
		chain:  newChain(),
		logger: zlog,
	}

	for _, opt := range opts {
		opt(db)
	}

	return db
}

func (f *ForkDB) SetLogger(logger *zap.Logger) {
	f.logger = logger
}

func (f *ForkDB) InitLIB(ref bstream.BlockRef) {
	if traceEnabled {
		f.logger.Debug("initializing lib", zap.Stringer("lib", ref))
	}

	f.Lock()
	f.libRef = ref
	f.Unlock()
}

func (f *ForkDB) HasLIB() (hasLIB bool) {
	f.RLock()
	hasLIB = f.libRef != nil && f.libRef.ID() != ""
	f.RUnlock()
	return
}

func (f *ForkDB) hasLIB() bool {
	return f.libRef != nil && f.libRef.ID() != ""
}

// TrySetLIB will move the lib if crawling from the given blockID up to the dposlibNum
// succeeds, giving us effectively the dposLIBID. It will perform the set LIB and set
// the new headBlockID
// unknown behaviour if it was already set ... maybe it explodes
func (f *ForkDB) TrySetLIB(headRef, previousRef bstream.BlockRef, libNum uint64) {
	if traceEnabled {
		f.logger.Debug("trying to set lib", zap.Stringer("head", headRef), zap.Stringer("previous", previousRef), zap.Uint64("lib_num", libNum))
	}

	f.RLock()
	if headRef.Num() == bstream.GetProtocolFirstStreamableBlock {
		// FIXME: Was bstream.GetProtocolGenesisBlock before forkdb refactoring, I feel current - 1 is a better choice, could it have unintended consequences?
		f.libRef = bstream.NewBlockRef(previousRef.ID(), bstream.GetProtocolFirstStreamableBlock-1)
		libNum = f.libRef.Num()

		f.logger.Info("candidate LIB received is first streamable block of chain, assuming it's the new LIB", zap.Stringer("lib", f.libRef))
	}

	libRef := f.blockInCurrentChain(headRef, libNum)
	f.RUnlock()

	if libRef.ID() == "" {
		f.logger.Debug("missing links to back fill cache to LIB num",
			zap.Stringer("head", headRef),
			zap.Stringer("previous_ref", previousRef),
			zap.Uint64("lib_num", libNum),
			zap.Uint64("get_protocol_first_block", bstream.GetProtocolFirstStreamableBlock),
		)
		return
	}

	// We need to have our read lock unlocked at this point!
	f.MoveLIB(libRef)
}

func (f *ForkDB) LIB() (lib bstream.BlockRef) {
	f.RLock()
	lib = f.libRef
	f.RUnlock()

	return
}

func (f *ForkDB) IsBehindLIB(blockNum uint64) (isBehind bool) {
	f.RLock()
	isBehind = blockNum <= f.libRef.Num()
	f.RUnlock()

	return
}

// chainSwitchSegments returns the list of block IDs that should be
// `undo`ne (in reverse chain order) and the list of blocks that
// should be `redo`ne (in chain order) for `blockID` (linking to
// `previousID`) to become the longest chain.
//
// This assumes you are querying for something that *is* the longest
// chain (or the to-become longest chain).
func (f *ForkDB) chainSwitchSegments(oldHeadRef, newHeadPreviousRef bstream.BlockRef) (undos, redos []bstream.BlockRef) {
	f.RLock()
	if !f.hasLIB() {
		f.RUnlock()
		return
	}

	undoNode := f.chain.getNode(oldHeadRef)
	redoNode := f.chain.getNode(newHeadPreviousRef)
	var redosReversed []bstream.BlockRef

	i := 0
	for {
		isJunction := nodeEquals(undoNode, redoNode)
		if traceEnabled {
			zlog.Debug("undo node equals redo node?", zap.Stringer("undo", undoNode), zap.Stringer("redo", redoNode), zap.Bool("equals", isJunction))
		}

		if isJunction {
			// We reached a junction point or both segment reached its end, nothing more do to
			break
		}

		// At this point, it's impossible that both `undoNode` and `redoNode` are `nil` due to check above.
		// That means we can assume at least one of the node will be `non-nil`.
		turn := i % 2
		if redoNode == nil || (undoNode != nil && turn == 0) {
			if traceEnabled {
				f.logger.Debug("moving to previous undo node")
			}

			undos = append(undos, undoNode.ref)
			undoNode = undoNode.prev
		} else if undoNode == nil || (redoNode != nil && turn == 1) {
			if traceEnabled {
				f.logger.Debug("moving to previous redo node")
			}

			redosReversed = append(redosReversed, redoNode.ref)
			redoNode = redoNode.prev
		}

		i++
	}
	f.RUnlock()

	// Reverse redos (if any)
	redoCount := len(redosReversed)
	if redoCount > 0 {
		redos = make([]bstream.BlockRef, redoCount)
		for i := 0; i < redoCount; i++ {
			redos[i] = redosReversed[redoCount-i-1]
		}
	}
	return
}

func (f *ForkDB) AddLink(blockRef, previousRef bstream.BlockRef, obj interface{}) (exists bool) {
	if traceEnabled {
		f.logger.Debug("adding link from previous to new", zap.Stringer("new", blockRef), zap.Stringer("previous", previousRef))
	}

	f.Lock()
	exists = f.chain.addLink(blockRef, obj, previousRef)
	f.Unlock()

	return
}

// BlockInCurrentChain finds the block_id at height `blockNum` under
// the requested `startAtBlockID` base block. Passing the head block id
// as `startAtBlockID` will tell you if the block num is part of the longuest
// chain.
func (f *ForkDB) BlockInCurrentChain(startAtBlock bstream.BlockRef, blockNum uint64) (inChainRef bstream.BlockRef) {
	f.RLock()
	inChainRef = f.blockInCurrentChain(startAtBlock, blockNum)
	f.RUnlock()

	return
}

func (f *ForkDB) blockInCurrentChain(startAtBlock bstream.BlockRef, blockNum uint64) bstream.BlockRef {
	for it := f.chain.getNode(startAtBlock); it != nil; it = it.prev {
		if it.ref.Num() == blockNum {
			return it.ref
		}
	}

	return bstream.BlockRefEmpty
}

// reversibleSegment returns the blocks between the previous
// irreversible Block ID and the given block ID.  The LIB is
// excluded and the given block ID is included in the results.
//
// Do not call this function is the .HasLIB() is false, as the result
// would make no sense.
//
// WARN: if the segment is broken by some unlinkable blocks, the
// return value is `nil`.
func (f *ForkDB) reversibleSegment(upToBlock bstream.BlockRef) (nodes []*node, err error) {
	f.RLock()
	defer f.RUnlock()

	if !f.hasLIB() {
		return nil, fmt.Errorf("the LIB ID is not defined and should have been")
	}

	cur := f.chain.getNode(upToBlock)
	if cur == nil {
		return nil, fmt.Errorf("unknown up to block %q", upToBlock)
	}

	if traceEnabled {
		f.logger.Debug("looking for reversible segment", zap.Stringer("from", f.libRef), zap.Stringer("to", upToBlock))
	}

	// We pre-allocate the array with some initial capacity to reduce allocations to a maximum
	var reversedNodes []*node
	capacity := int64(cur.Num()) - int64(f.libRef.Num())
	if capacity > 8 {
		reversedNodes = make([]*node, 0, capacity)
	}

	for {
		if cur.Num() > bstream.GetProtocolFirstStreamableBlock && cur.Num() < f.libRef.Num() {
			f.logger.Debug("forkdb linking past known irreversible block", zap.Stringer("lib", f.libRef), zap.Stringer("block", cur))
			return nil, nil
		}

		if cur.ID() == f.libRef.ID() {
			break
		}

		reversedNodes = append(reversedNodes, cur)

		prev := cur.prev
		if prev == nil {
			f.logger.Debug("forkdb unlinkable block",
				zap.Stringer("block", cur),
				zap.Stringer("from_block", upToBlock),
				zap.Stringer("lib", f.libRef),
			)
			return nil, nil
		}

		cur = prev
	}

	// Reverse sort `blocks`
	nodes = make([]*node, len(reversedNodes))
	for i := 0; i < len(reversedNodes); i++ {
		nodes[i] = reversedNodes[len(reversedNodes)-i-1]
	}
	return
}

func (f *ForkDB) stalledSegmentsInNodes(nodes []*node, sorted bool) (out []*node) {
	if len(nodes) == 0 {
		return
	}

	inSegment := make(map[string]bool)
	for _, node := range nodes {
		inSegment[node.ID()] = true
	}

	if traceEnabled {
		f.logger.Debug("checking for stalled segments", zap.Reflect("in_segment", inSegment))
	}

	for _, link := range nodes {
		if len(link.nexts) == 1 {
			continue
		}

		// We have more than one follower, some of them are stalled segment
		for _, next := range link.nexts {
			if _, found := inSegment[next.ID()]; found {
				// Skip a next link that is actually in our segment
				continue
			}

			if traceEnabled {
				f.logger.Debug("walking stalled segment", zap.Stringer("root", next))
			}

			out = append(out, f.walkStalledSegment(next)...)
		}
	}

	if sorted {
		sort.Slice(out, func(i, j int) bool {
			left := out[i]
			right := out[j]
			if left.Num() == right.Num() {
				return left.ID() < right.ID()
			}

			return left.Num() < right.Num()
		})
	}

	return out
}

func (f *ForkDB) walkStalledSegment(root *node) (out []*node) {
	if root == nil {
		return nil
	}

	cur := root
	for {
		out = append(out, cur)
		if len(cur.nexts) == 0 {
			return out
		}

		if len(cur.nexts) == 1 {
			cur = cur.nexts[0]
			continue
		}

		// Recursively walk multi segments
		for _, next := range cur.nexts {
			out = append(out, f.walkStalledSegment(next)...)
		}
		return out
	}
}

// checkNewIrreversibleSegment returns segments upon passing the
// newDposLIBID that are irreversible and stale. If there was no new
// segment, `hasNew` will be false. WARN: this method can only be
// called when `HasLIB()` is true.  Otherwise, it panics.
func (f *ForkDB) checkNewIrreversibleSegment(newLIB bstream.BlockRef) (hasNew bool, irreversibleSegment, staleNodes []*node, err error) {
	if bstream.EqualsBlockRefs(f.libRef, newLIB) {
		return false, nil, nil, nil
	}

	irreversibleSegment, err = f.reversibleSegment(newLIB)
	if err != nil {
		return false, nil, nil, fmt.Errorf("reversible segment: %w", err)
	}

	if len(irreversibleSegment) == 0 {
		return false, nil, nil, nil
	}

	return true, irreversibleSegment, f.stalledSegmentsInNodes(irreversibleSegment, true), nil
}

func (f *ForkDB) MoveLIB(newLIB bstream.BlockRef) error {
	if traceEnabled {
		f.logger.Debug("moving lib", zap.Stringer("from", f.libRef), zap.Stringer("to", newLIB))
	}

	f.Lock()
	defer f.Unlock()

	oldNode := f.chain.getNode(f.libRef)
	if oldNode == nil {
		f.logger.Debug("moving to an unknown old node")
	}

	curNode := f.chain.getNode(newLIB)
	if curNode == nil {
		return fmt.Errorf("unable to find new lib node %q", newLIB)
	}

	if nodeEquals(oldNode, curNode) {
		f.logger.Debug("moving lib is a no-op, previous and new LIB points to same node")
		return nil
	}

	nodes := f.chain.nodesBetween(oldNode, curNode)
	stalledInNodes := f.stalledSegmentsInNodes(append(nodes, curNode), false)

	if traceEnabled {
		f.logger.Debug("removing now past LIB nodes", zap.Int("node_count", len(nodes)), zap.Int("stalled_node_count", len(stalledInNodes)))
	}

	for _, node := range nodes {
		f.chain.removeLink(node)
	}

	for _, stalledNode := range stalledInNodes {
		f.chain.removeLink(stalledNode)
	}

	f.libRef = newLIB
	if curNode != nil {
		curNode.prev = nil
	}

	return nil
}

func (f *ForkDB) nodeForRef(ref bstream.BlockRef) (out *node) {
	return f.chain.getNode(ref)
}
