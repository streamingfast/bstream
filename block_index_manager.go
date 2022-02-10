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

// BlockIndexesManager works with an 'irreversible index' (mapping block numbers their canonical IDs)
// It can provide an ordered list of all 'irreversible' blocks over a certain range.
// Given a BlockIndexProvider, it will also filter out non-matching blocks from that ordered list
type BlockIndexesManager struct {
	sync.RWMutex

	irrIdxStore         dstore.Store
	irrIdxPossibleSizes []uint64
	blockIndexProvider  BlockIndexProvider

	// irreversible index state
	noMoreIrrIdx                 bool // we already failed trying to load next range
	irrIdxLoadedUpperBoundary    uint64
	loadedUpperIrreversibleBlock *pbblockmeta.BlockRef

	// combined indexes state
	nextBlockRefs             []*pbblockmeta.BlockRef
	pendingPreprocessedBlocks map[uint64]*PreprocessedBlock
}

// NewBlockIndexesManager loads the indexes from startBlockNum up to a range containing some nextBlockRefs or until the very last index, if no block match
// It returns nil if requiredBlock is missing from the first or if no index exists at startBlockNum
// if the blockIndexProvider is nil, it will serve all irreversible blocks, from the irreversibility index
func NewBlockIndexesManager(store dstore.Store, bundleSizes []uint64, startBlockNum uint64, requiredBlock BlockRef, blockIndexProvider BlockIndexProvider) *BlockIndexesManager {

	ind := &BlockIndexesManager{
		irrIdxStore:               store,
		irrIdxPossibleSizes:       bundleSizes,
		blockIndexProvider:        blockIndexProvider,
		pendingPreprocessedBlocks: make(map[uint64]*PreprocessedBlock),
	}

	found := ind.loadRange(startBlockNum)
	if !found {
		return nil
	}

	if requiredBlock != nil && requiredBlock.ID() != "" {

		// load more blocks if cursor HEAD (requiredBlock) is in another index than cursor LIB (startBlockNum)
		if requiredBlock.Num() > ind.irrIdxLoadedUpperBoundary {
			ind.loadRangesUntil(requiredBlock.Num())
		}

		var foundMatching bool
		for _, b := range ind.nextBlockRefs {
			if b.BlockID == requiredBlock.ID() {
				foundMatching = true
			}
		}
		if !foundMatching {
			return nil
		}
	} else {
		// find at least a single block in the index, or crawl to the end
		ind.loadRangesUntil(0)
	}
	return ind

}

// filterAgainstExtraIndexProvider receives "in", an array of blocks, ex:  [10, 13, 14, 15]
// for the parts of "in" that are within the boundaries of the extraIndex, we ask it which ones match, so we can return, ex: [10, 14]
// for the parts of "in" that are outside the boundaries of the extraIndex, or if we encounter any error while querying extraIndex, we will return the full input array,
// therefore not performing extra filtering.
func (s *BlockIndexesManager) filterAgainstExtraIndexProvider(in []*pbblockmeta.BlockRef) (out []*pbblockmeta.BlockRef) {
	if s.blockIndexProvider == nil || len(in) == 0 {
		return in
	}
	if !s.blockIndexProvider.WithinRange(in[0].BlockNum) { // index stops before next irreversible blocks
		zlog.Debug("removing extraIndexProvider because not within range", zap.Uint64("in_0_blocknum", in[0].BlockNum))
		s.blockIndexProvider = nil
		return in
	}

	var firstUnindexedBlock uint64

	var nextMatching uint64
	match, err := s.blockIndexProvider.Matches(in[0].BlockNum)
	if err != nil {
		zlog.Warn("removing extraIndexProvider because we got an error", zap.Error(err))
		s.blockIndexProvider = nil
		return in
	}
	if match {
		nextMatching = in[0].BlockNum
	} else {
		next, passedIndexBoundary, err := s.blockIndexProvider.NextMatching(in[0].BlockNum)
		if err != nil {
			zlog.Warn("removing extraIndexProvider because we got an error", zap.Error(err))
			s.blockIndexProvider = nil
			return in
		}
		if passedIndexBoundary {
			firstUnindexedBlock = next
		} else {
			nextMatching = next
		}
	}

	for i := 0; i < len(in); i++ {
		if firstUnindexedBlock != 0 && in[i].BlockNum >= firstUnindexedBlock { // we are passed index boundary, letting all further blocks pass through
			out = append(out, in[i])
			continue
		}

		if in[i].BlockNum < nextMatching {
			continue
		}

		if in[i].BlockNum == nextMatching {
			out = append(out, in[i])
		}

		next, passedIndexBoundary, err := s.blockIndexProvider.NextMatching(in[i].BlockNum)
		if err != nil {
			zlog.Warn("removing extraIndexProvider because we got an error", zap.Error(err))
			s.blockIndexProvider = nil
			return in
		}

		if passedIndexBoundary {
			firstUnindexedBlock = next
		} else {
			nextMatching = next
		}
	}

	return
}

// ProcessOrderedSegment will either process blk immediately or process it with other blocks the next time it is called
// ex: ProcessOrderedSegment(b3) (does nothing), ProcessOrderedSegment(b2) -> calls handler.ProcessBlock(b2), then handler.ProcessBlock(b3)
func (s *BlockIndexesManager) ProcessOrderedSegment(blk *PreprocessedBlock, handler Handler) (lastProcessedBlock *PreprocessedBlock, indexedRangeComplete bool, err error) {

	if len(s.nextBlockRefs) == 0 {
		// quickly trigger shutdown of that source with indexedRangeComplete==true
		indexedRangeComplete = true
		return
	}

	var toProcess []*PreprocessedBlock
	if blk.ID() == s.nextBlockRefs[0].BlockID {
		toProcess = append(toProcess, blk)
	} else {
		s.pendingPreprocessedBlocks[blk.Num()] = blk
	}

	s.Lock()
	for i, b := range s.nextBlockRefs {
		if blk.ID() == b.BlockID {
			s.nextBlockRefs = removeIndex(s.nextBlockRefs, i)
			break // ensure we don't reuse index 'i' after removeIndex
		}
	}
	s.Unlock()

	if len(toProcess) == 0 {
		return
	}

	nextBoundary := s.irrIdxLoadedUpperBoundary + 1
	s.RLock()
	noNextBlockRefs := len(s.nextBlockRefs) == 0
	if !noNextBlockRefs {
		nextBoundary = s.nextBlockRefs[0].BlockNum
	}
	s.RUnlock()

	var reorder []*PreprocessedBlock
	for i, b := range s.pendingPreprocessedBlocks {
		if i > blk.Num() && i < nextBoundary {
			reorder = append(reorder, b)
		}
	}
	sort.Slice(reorder, func(i, j int) bool { return reorder[i].Num() < reorder[j].Num() })
	for _, b := range reorder {
		toProcess = append(toProcess, b)
		delete(s.pendingPreprocessedBlocks, b.Num())
	}

	for _, b := range toProcess {
		if e := handler.ProcessBlock(b.Block, b.Obj); e != nil {
			err = e
			return
		}
		lastProcessedBlock = b
	}

	// fill the nextBlockRefs if needed
	if noNextBlockRefs {
		if len(s.pendingPreprocessedBlocks) != 0 {
			panic("bug in irrBlocksIndex reorder or missing blocks in your store")
		}
		s.loadRangesUntil(0)
		s.RLock()
		indexedRangeComplete = (len(s.nextBlockRefs) == 0)
		s.RUnlock()
	}

	return
}

// Skip(BlockRef) should be called from a preprocessor threads, to know if a block needs to be skipped
func (s *BlockIndexesManager) Skip(blk BlockRef) bool {
	if !s.withinIndexRange(blk.Num()) {
		return true
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

// NextMergedBlocksBase() informs about the next block file that should be read using a Filesource
// if a block (ex: 123) was not part of blocks file 100, NextMergedBlocksBase() will always show 100, even if your last read file is 300
// to optimize sprase replays, you should periodically call NextMergedBlocksBase()
// and shutdown/create a new filesource to read blocks from there if the last read file is too far away in the future
func (s *BlockIndexesManager) NextMergedBlocksBase() (baseNum uint64, lib BlockRef, hasIndex bool) {
	s.RLock()
	defer s.RUnlock()

	var nextWantedBlockNum uint64
	if len(s.nextBlockRefs) > 0 {
		hasIndex = true
		nextWantedBlockNum = s.nextBlockRefs[0].BlockNum
	} else {
		nextWantedBlockNum = s.irrIdxLoadedUpperBoundary + 1
	}

	baseNum = lowBoundary(nextWantedBlockNum, 100)

	if !hasIndex {
		if l := s.loadedUpperIrreversibleBlock; l != nil {
			lib = BasicBlockRef{l.BlockID, l.BlockNum}
		}
	}

	return
}

func (s *BlockIndexesManager) withinIndexRange(blockNum uint64) bool {
	if blockNum <= s.irrIdxLoadedUpperBoundary {
		return true
	}
	fmt.Println("CALLING LOAD RANGES UNTIL", blockNum, s.irrIdxLoadedUpperBoundary)
	s.loadRangesUntil(blockNum)
	return blockNum <= s.irrIdxLoadedUpperBoundary
}

func (s *BlockIndexesManager) loadRangesUntil(blockNum uint64) {
	if s.noMoreIrrIdx {
		return
	}

	s.Lock()
	defer s.Unlock()

	for {
		if blockNum == 0 && len(s.nextBlockRefs) != 0 {
			return
		}
		if blockNum != 0 && s.irrIdxLoadedUpperBoundary >= blockNum {
			return
		}

		next := s.irrIdxLoadedUpperBoundary + 1
		if found := s.loadRange(next); !found {
			s.noMoreIrrIdx = true
			return
		}

	}
}

// locked
func (s *BlockIndexesManager) loadRange(blockNum uint64) (found bool) {
	// should load each index until we reached ...

	blockIDs, loadedUpperBoundary, found := loadRangeIndex(blockNum, s.irrIdxPossibleSizes, s.irrIdxStore)
	if found {
		for _, b := range blockIDs {
			s.nextBlockRefs = append(s.nextBlockRefs, b)
		}
		s.irrIdxLoadedUpperBoundary = loadedUpperBoundary
		s.bumpLoadedUpperIrreversibleBlock()
		s.nextBlockRefs = s.filterAgainstExtraIndexProvider(s.nextBlockRefs)
		return true
	}

	return false
}

func (s *BlockIndexesManager) bumpLoadedUpperIrreversibleBlock() {
	if len(s.nextBlockRefs) != 0 {
		upperBlock := s.nextBlockRefs[len(s.nextBlockRefs)-1]
		if s.loadedUpperIrreversibleBlock == nil ||
			upperBlock.BlockNum > s.loadedUpperIrreversibleBlock.BlockNum {
			s.loadedUpperIrreversibleBlock = upperBlock
		}
	}
}

func loadRangeIndex(startBlockNum uint64, bundleSizes []uint64, store dstore.Store) (blockRefs []*pbblockmeta.BlockRef, loadedUpperBoundary uint64, found bool) {
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

	if resp.BlockRefs == nil {
		return []*pbblockmeta.BlockRef{}, nil
	}
	return resp.BlockRefs, nil
}

func lowBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod)
}
