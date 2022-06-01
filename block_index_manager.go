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

	ctx                 context.Context
	irrIdxStore         dstore.Store
	irrIdxPossibleSizes []uint64
	blockIndexProvider  BlockIndexProvider

	// irreversible index state
	noMoreIrrIdx                 bool // we already failed trying to load next range
	irrIdxLoadedUpperBoundary    uint64
	loadedUpperIrreversibleBlock *pbblockmeta.BlockRef
	cursorBlock                  BlockRef
	stopBlockNum                 uint64

	forceIncludeNextBlock      bool // if true, next block >= forceIncludeNextBlockAfter will never be filtered out (useful for start block or block right after cursor)
	forceIncludeNextBlockAfter uint64

	// combined indexes state
	nextBlockRefs             []*pbblockmeta.BlockRef
	pendingPreprocessedBlocks map[uint64]*PreprocessedBlock
}

// NewBlockIndexesManager loads the indexes from startBlockNum up to a range containing some nextBlockRefs or until the very last index, if no block match
// It returns nil if cursorBlock is missing from the index or if no index exists at startBlockNum
// if the blockIndexProvider is nil, it will serve all irreversible blocks, from the irreversibility index
func NewBlockIndexesManager(ctx context.Context, irrIdxStore dstore.Store, irrIdxPossibleSizes []uint64, startBlockNum uint64, stopBlockNum uint64, cursorBlock BlockRef, blockIndexProvider BlockIndexProvider) *BlockIndexesManager {

	if !ensureIndexesCover(ctx, startBlockNum, cursorBlock, irrIdxPossibleSizes, irrIdxStore) {
		return nil
	}

	ind := &BlockIndexesManager{
		ctx:                       ctx,
		irrIdxStore:               irrIdxStore,
		irrIdxPossibleSizes:       irrIdxPossibleSizes,
		blockIndexProvider:        blockIndexProvider,
		stopBlockNum:              stopBlockNum,
		cursorBlock:               cursorBlock,
		pendingPreprocessedBlocks: make(map[uint64]*PreprocessedBlock),
	}

	if err := ind.initialize(startBlockNum); err != nil {
		zlog.Error("error initalizing block_index_manager", zap.Error(err))
		return nil
	}

	return ind
}

// ensureIndexesCover checks that startBlockNum is in range of an irreversible index
// if cursorBlock is set, it also checks that its block ID is present in the index
func ensureIndexesCover(ctx context.Context, startBlockNum uint64, cursorBlock BlockRef, irrIdxPossibleSizes []uint64, irrIdxStore dstore.Store) bool {
	blocks, loadedUpperBoundary, found := loadIrreversibleIndex(ctx, startBlockNum, irrIdxPossibleSizes, irrIdxStore)
	if !found {
		return false
	}
	if cursorBlock == nil {
		return true
	}

	if loadedUpperBoundary > cursorBlock.Num() {
		for _, blk := range blocks {
			if blk.BlockID == cursorBlock.ID() {
				return true
			}
		}
		return false
	}

	blocks, _, found = loadIrreversibleIndex(ctx, cursorBlock.Num(), irrIdxPossibleSizes, irrIdxStore)
	if !found {
		return false
	}
	for _, blk := range blocks {
		if blk.BlockID == cursorBlock.ID() {
			return true
		}
	}
	return false
}

func (s *BlockIndexesManager) initialize(startBlockNum uint64) error {
	if s.blockIndexProvider == nil {
		found := s.loadRange(startBlockNum)
		if !found {
			return fmt.Errorf("error initializing block indexes manager: cannot load irreversible index containing start block")
		}
		if len(s.nextBlockRefs) == 0 { // ex: startBlockNum is on block 99 which does not exist on this chain, keep looking
			s.loadRangesUntilMatch()
		}
		return nil
	}

	// we will always assume match on startBlockNum
	_, err := s.blockIndexProvider.Matches(s.ctx, startBlockNum)
	if err != nil {
		zlog.Error("cannot lookup Matches on blockIndexProvider",
			zap.Error(err),
			zap.Uint64("start_block_num", startBlockNum),
		)
		s.disableBlockIndexProvider()
		return s.initialize(startBlockNum)
	}

	found := s.loadRange(startBlockNum)
	if !found { // happens if indexProvider goes beyond actual irreversible index, not an ideal case
		zlog.Debug("cannot find irreversible index for start block, disabling irreversible indexes")
		s.disableBlockIndexProvider()
		return s.initialize(startBlockNum)
	}

	if s.cursorBlock == nil {
		s.forceIncludeNextBlock = true
		if startBlockNum > 0 {
			s.forceIncludeNextBlockAfter = startBlockNum - 1
		}
	}
	s.filterAgainstExtraIndexProvider()
	if len(s.nextBlockRefs) == 0 {
		return fmt.Errorf("block index provider returned no block where it should have")
	}
	return nil
}

func (s *BlockIndexesManager) queryHighestIrreversibleIndex(low, excludedHigh uint64) (nextStartBlock uint64, lib *pbblockmeta.BlockRef, err error) {

	movingStartBlock := low
	for {
		if movingStartBlock >= excludedHigh {
			break
		}

		blocks, loadedUpperBoundary, found := loadIrreversibleIndex(s.ctx, movingStartBlock, s.irrIdxPossibleSizes, s.irrIdxStore)
		if !found {
			break
		}

		if loadedUpperBoundary >= excludedHigh {
			var highestBlock *pbblockmeta.BlockRef
			for _, blk := range blocks {
				if blk.BlockNum < excludedHigh {
					highestBlock = blk
				}
			}
			lib = highestBlock
			nextStartBlock = excludedHigh
			break
		}

		if len(blocks) == 0 {
			break
		}
		lib = blocks[len(blocks)-1]
		nextStartBlock = loadedUpperBoundary

		// hit the last index
		if loadedUpperBoundary <= movingStartBlock {
			break
		}

		movingStartBlock += (loadedUpperBoundary - movingStartBlock)
	}
	if nextStartBlock == 0 || lib == nil {
		err = fmt.Errorf("couldn't load any irreversible index")
	}
	return

}

func (s *BlockIndexesManager) disableBlockIndexProvider() {
	zlog.Debug("disabling block index provider")
	s.blockIndexProvider = nil
}

// filterAgainstExtraIndexProvider receives "in", an array of blocks, ex:  [10, 13, 14, 15]
// for the parts of "in" that are within the boundaries of the extraIndex, we ask it which ones match, so we can return, ex: [10, 14]
// for the parts of "in" that are outside the boundaries of the extraIndex, or if we encounter any error while querying extraIndex, we will return the full input array,
// therefore not performing extra filtering.
func (s *BlockIndexesManager) filterAgainstExtraIndexProvider() {
	in := s.nextBlockRefs
	if s.blockIndexProvider == nil || len(in) == 0 {
		return
	}

	var out []*pbblockmeta.BlockRef

	if !s.blockIndexProvider.WithinRange(s.ctx, in[0].BlockNum) { // index stops before next irreversible blocks
		s.disableBlockIndexProvider()
		return
	}

	exclusiveUpTo := in[len(in)-1].BlockNum + 1

	var nextIsPassedIndexBoundary bool
	var expectedNext uint64
	match, err := s.blockIndexProvider.Matches(s.ctx, in[0].BlockNum)
	if err != nil {
		s.disableBlockIndexProvider()
		return
	}
	if match {
		expectedNext = in[0].BlockNum
	} else if s.forceIncludeNextBlock && in[0].BlockNum > s.forceIncludeNextBlockAfter {
		expectedNext = in[0].BlockNum
		s.forceIncludeNextBlock = false
	} else {
		next, passedIndexBoundary, err := s.blockIndexProvider.NextMatching(s.ctx, in[0].BlockNum, exclusiveUpTo)
		if err != nil {
			s.disableBlockIndexProvider()
			return
		}
		expectedNext = next
		if passedIndexBoundary {
			nextIsPassedIndexBoundary = true
		}
	}

	effectiveStopBlock := ^uint64(0) //max uint64
	if s.stopBlockNum > 0 {
		effectiveStopBlock = s.stopBlockNum
	}

	for i := 0; i < len(in); i++ {
		isCursor := s.cursorBlock != nil && in[i].BlockID == s.cursorBlock.ID()
		if in[i].BlockNum >= effectiveStopBlock {
			out = append(out, in[i]) // we send the stop block even if it won't match, then stop
			break
		}
		if s.forceIncludeNextBlock && in[i].BlockNum > s.forceIncludeNextBlockAfter {
			s.forceIncludeNextBlock = false
			expectedNext = in[i].BlockNum // proceed as if we were expecting this one
		}
		if in[i].BlockNum < expectedNext && !isCursor { // skip all blocks below expectedNext except the cursor
			continue
		}

		if nextIsPassedIndexBoundary && in[i].BlockNum >= expectedNext {
			out = append(out, in[i]) // everything passed index boundaries must flow
			continue
		}

		if isCursor { // cursor block itself won't flow (already sent...), the next one will as if it was the 'start' block
			s.forceIncludeNextBlock = true
			s.forceIncludeNextBlockAfter = in[i].BlockNum
		} else if in[i].BlockNum == expectedNext { // expected block must flow, obviously
			out = append(out, in[i])
		}

		next, passedIndexBoundary, err := s.blockIndexProvider.NextMatching(s.ctx, in[i].BlockNum, exclusiveUpTo)
		if err != nil {
			s.disableBlockIndexProvider()
			return
		}
		expectedNext = next

		if passedIndexBoundary {
			nextIsPassedIndexBoundary = true
		}
	}

	s.nextBlockRefs = out
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
			return nil, false, fmt.Errorf("bug in irrBlocksIndex reorder or missing blocks in your store")
		}
		s.loadRangesUntilMatch()
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
	if s.stopBlockNum != 0 && blockNum > s.stopBlockNum {
		return false // never trigger loadRange when looking at blocks passed stopBlock in preprocess phase
	}
	if blockNum <= s.irrIdxLoadedUpperBoundary {
		return true
	}
	s.loadRangesUntil(blockNum)
	return blockNum <= s.irrIdxLoadedUpperBoundary
}

func (s *BlockIndexesManager) loadRangesUntilMatch() {
	if s.noMoreIrrIdx {
		return
	}

	s.Lock()
	defer s.Unlock()

	for {
		if len(s.nextBlockRefs) != 0 {
			return
		}

		next := s.nextInterestingRange()
		if found := s.loadRange(next); !found {
			zlog.Debug("irreversible index seems to end before the block index provider", zap.Uint64("missing_irreversible_range", next), zap.Uint64("last_loaded_upper_boundary", s.irrIdxLoadedUpperBoundary))
			s.nextBlockRefs = nil
			s.noMoreIrrIdx = true

			upperBoundary, lib, err := s.queryHighestIrreversibleIndex(s.irrIdxLoadedUpperBoundary, next)
			if err != nil {
				zlog.Debug("error querying highest irreversible index", zap.Error(err))
				return
			}
			s.loadedUpperIrreversibleBlock = lib
			s.irrIdxLoadedUpperBoundary = upperBoundary
			return
		}
		s.filterAgainstExtraIndexProvider()
	}
}

func (s *BlockIndexesManager) nextInterestingRange() uint64 {
	defaultValue := s.irrIdxLoadedUpperBoundary + 1
	if s.blockIndexProvider == nil {
		return defaultValue
	}

	next, endReached, err := s.blockIndexProvider.NextMatching(s.ctx, s.irrIdxLoadedUpperBoundary, s.stopBlockNum)
	if err != nil {
		s.disableBlockIndexProvider()
		return defaultValue
	}
	if endReached {
		s.disableBlockIndexProvider()
	}
	return next
}

func (s *BlockIndexesManager) loadRangesUntil(blockNum uint64) {
	if s.noMoreIrrIdx {
		return
	}

	s.Lock()
	defer s.Unlock()

	for {
		if s.irrIdxLoadedUpperBoundary >= blockNum {
			return
		}

		next := s.nextInterestingRange()

		found := s.loadRange(next)
		if !found {
			s.noMoreIrrIdx = true
			return
		}
		s.filterAgainstExtraIndexProvider()
	}
}

//loadRange will load a range of blocks starting at blockNum (ex: [1320...9999])
func (s *BlockIndexesManager) loadRange(blockNum uint64) (found bool) {
	blocks, loadedUpperBoundary, found := loadIrreversibleIndex(s.ctx, blockNum, s.irrIdxPossibleSizes, s.irrIdxStore)
	if found {
		for _, b := range blocks {
			if b.BlockNum >= blockNum {
				s.nextBlockRefs = append(s.nextBlockRefs, b)
			}
		}
		s.irrIdxLoadedUpperBoundary = loadedUpperBoundary
		s.bumpLoadedUpperIrreversibleBlock()
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

func loadIrreversibleIndex(ctx context.Context, startBlockNum uint64, irrIdxPossibleSizes []uint64, store dstore.Store) (blockRefs []*pbblockmeta.BlockRef, loadedUpperBoundary uint64, found bool) {
	for _, size := range irrIdxPossibleSizes {
		baseBlockNum := lowBoundary(startBlockNum, size)
		fetchedBlockRefs, err := getIrreversibleIndex(ctx, baseBlockNum, store, size)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
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

func getIrreversibleIndex(ctx context.Context, baseBlockNum uint64, store dstore.Store, bundleSize uint64) ([]*pbblockmeta.BlockRef, error) {
	filename := fmt.Sprintf("%010d.%d.irr.idx", baseBlockNum, bundleSize)
	reader, err := store.OpenObject(ctx, filename)
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
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("cannot properly close file %s: %w", filename, err)
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
