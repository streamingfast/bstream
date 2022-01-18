package bstream

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"sort"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/dstore"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	"go.uber.org/zap"
)

// Workflow

// newIrreversibleBlocksIndex loads the indexes from startBlockNum until either
//   1. it at least finds a block in nextBlockRefs
//   2. it reaches the end (and sets noMoreIndexes to true)

// to optimize sprase replays, you should periodically call NextBaseBlock()
// and shutdown/create a new filesource to read blocks from there if the last read file is too far away

// if a block (ex: 123) was not part of blocks file 100, NextBaseBlock() will always show 100, even if your last read file is 300
// so only trigger the optimization if the NextBaseBlock() is in the future, not in the past

type irrBlocksIndex struct {
	sync.RWMutex

	noMoreIndexes                bool // we already failed trying to load next range
	loadedUpperBoundary          uint64
	loadedUpperIrreversibleBlock *pbblockmeta.BlockRef

	nextBlockRefs []*pbblockmeta.BlockRef
	store         dstore.Store
	bundleSizes   []uint64
	disordered    map[uint64]*PreprocessedBlock // blocks sent to Reorder but not out yet
}

type BlockIndex interface {
	// Skip(BlockRef) should be called from a preprocessor threads, to know if a block needs to be skipped
	Skip(BlockRef) bool

	// NextBaseBlock() informs about the next block file that should be read using a Filesource
	NextBaseBlock() (baseNum uint64, lib BlockRef, hasIndex bool)

	// Reorder should be called from ProcessBlock, it either:
	//    1. keeps unordered blocks for later use (and returns nil)
	//    2. sends you back your input block (with optionally extra blocks that had been set aside)
	Reorder(blk *PreprocessedBlock) []*PreprocessedBlock
}

// returns nil if requiredBlock isn't there or if no index exists at startBlockNum
func newIrreversibleBlocksIndex(store dstore.Store, bundleSizes []uint64, startBlockNum uint64, requiredBlock BlockRef) *irrBlocksIndex {

	sort.Slice(bundleSizes, func(i, j int) bool { return bundleSizes[i] > bundleSizes[j] })

	blockRefs, loadedUpperBoundary, found := loadRange(startBlockNum, bundleSizes, store)
	if !found {
		return nil
	}

	if requiredBlock != nil && requiredBlock.ID() != "" {
		var foundMatching bool
		for _, b := range blockRefs {
			if b.BlockID == requiredBlock.ID() {
				foundMatching = true
			}
		}
		if !foundMatching {
			return nil
		}
	}

	ind := &irrBlocksIndex{
		store:               store,
		bundleSizes:         bundleSizes,
		loadedUpperBoundary: loadedUpperBoundary,
		nextBlockRefs:       blockRefs,
		disordered:          make(map[uint64]*PreprocessedBlock),
	}
	if len(ind.nextBlockRefs) == 0 {
		// ensure we either have at least one blockref or have gone through the whole available ranges
		ind.loadRangesUntil(0)
	}

	return ind

}

// Reorder assumes that Skip() has returned false, it should not receive blocks that are "to be skipped"
func (s *irrBlocksIndex) Reorder(blk *PreprocessedBlock) (out []*PreprocessedBlock) {

	if len(s.nextBlockRefs) == 0 {
		// FIXME
		panic("reorder called but no nextBlockRefs exists")
	}

	if blk.ID() == s.nextBlockRefs[0].BlockID {
		out = append(out, blk)
	} else {
		s.disordered[blk.Num()] = blk
	}

	s.RLock()
	for i, b := range s.nextBlockRefs {
		if blk.ID() == b.BlockID {
			s.nextBlockRefs = removeIndex(s.nextBlockRefs, i)
			break // ensure we don't reuse index 'i' after removeIndex
		}
	}
	s.RUnlock()

	if len(out) == 0 {
		return
	}

	nextBoundary := s.loadedUpperBoundary + 1
	if len(s.nextBlockRefs) != 0 {
		nextBoundary = s.nextBlockRefs[0].BlockNum
	}
	var reorder []*PreprocessedBlock
	for i, b := range s.disordered {
		if i > blk.Num() && i < nextBoundary {
			reorder = append(reorder, b)
		}
	}
	sort.Slice(reorder, func(i, j int) bool { return reorder[i].Num() < reorder[j].Num() })
	for _, b := range reorder {
		out = append(out, b)
		delete(s.disordered, b.Num())
	}

	// fill the nextBlockRefs if needed
	if len(s.nextBlockRefs) == 0 {
		if len(s.disordered) != 0 {
			panic("but in irrBlocksIndex reorder or missing blocks in your store")
		}
		s.loadRangesUntil(0)
	}

	return
}

// multi-threaded
func (s *irrBlocksIndex) Skip(blk BlockRef) bool {
	if !s.withinIndexRange(blk.Num()) {
		return false
	}

	s.RLock()
	defer s.RUnlock()
	for _, b := range s.nextBlockRefs {
		if blk.ID() == b.BlockID {
			return false
		}
	}
	return true

}

func removeIndex(s []*pbblockmeta.BlockRef, index int) []*pbblockmeta.BlockRef {
	ret := make([]*pbblockmeta.BlockRef, 0)
	ret = append(ret, s[:index]...)
	return append(ret, s[index+1:]...)
}

// NextBaseBlock additionally includes a lib hasIndex is false so you can bootstrap
// next source with a forkable
func (s *irrBlocksIndex) NextBaseBlock() (baseNum uint64, lib BlockRef, hasIndex bool) {
	s.RLock()
	defer s.RUnlock()

	var nextWantedBlockNum uint64
	if len(s.nextBlockRefs) > 0 {
		hasIndex = true
		nextWantedBlockNum = s.nextBlockRefs[0].BlockNum
	} else {
		nextWantedBlockNum = s.loadedUpperBoundary + 1
	}

	baseNum = lowBoundary(nextWantedBlockNum, 100)

	if !hasIndex {
		if l := s.loadedUpperIrreversibleBlock; l != nil {
			lib = BasicBlockRef{l.BlockID, l.BlockNum}
		}
	}

	return
}

func (s *irrBlocksIndex) withinIndexRange(blockNum uint64) bool {
	if blockNum <= s.loadedUpperBoundary {
		return true
	}
	s.loadRangesUntil(blockNum)
	return blockNum <= s.loadedUpperBoundary
}

func (s *irrBlocksIndex) loadRangesUntil(blockNum uint64) {
	if s.noMoreIndexes {
		return
	}

	s.Lock()
	defer s.Unlock()

	for {
		if blockNum == 0 && len(s.nextBlockRefs) != 0 {
			return
		}
		if blockNum != 0 && s.loadedUpperBoundary >= blockNum {
			return
		}

		next := s.loadedUpperBoundary + 1
		if found := s.loadRange(next); !found {
			s.noMoreIndexes = true
			return
		}

	}
}

func (s *irrBlocksIndex) loadRange(blockNum uint64) (found bool) {
	// should load each index until we reached ...

	blockIDs, loadedUpperBoundary, found := loadRange(blockNum, s.bundleSizes, s.store)
	if found {
		for _, b := range blockIDs {
			s.nextBlockRefs = append(s.nextBlockRefs, b)
		}
		s.loadedUpperBoundary = loadedUpperBoundary
		return true
	}

	return false
}

func loadRange(startBlockNum uint64, bundleSizes []uint64, store dstore.Store) (blockRefs []*pbblockmeta.BlockRef, loadedUpperBoundary uint64, found bool) {
	for _, size := range bundleSizes {
		baseBlockNum := lowBoundary(startBlockNum, size)
		fetchedBlockRefs, err := getIrreversibleIndex(baseBlockNum, store, size)
		if err != nil {
			zlog.Warn("error fetching irreversible index",
				zap.Uint64("base_block_num", baseBlockNum),
				zap.Error(err),
			)
			continue
		}

		if fetchedBlockRefs != nil {
			found = true
			for _, b := range fetchedBlockRefs {
				if b.BlockNum >= startBlockNum {
					blockRefs = append(blockRefs, b)
				}
			}
			loadedUpperBoundary = baseBlockNum + size - 1
			return
		}
	}
	return
}

func getIrreversibleIndex(baseBlockNum uint64, store dstore.Store, bundleSize uint64) ([]*pbblockmeta.BlockRef, error) {
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

	if resp.BlockRefs == nil { //FIXME is this the right way?
		return []*pbblockmeta.BlockRef{}, nil
	}
	return resp.BlockRefs, nil
}

func lowBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod)
}
