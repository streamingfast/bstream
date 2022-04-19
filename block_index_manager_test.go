package bstream

import (
	"context"
	"sort"
	"testing"

	"github.com/streamingfast/dstore"
	pb "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/test-go/testify/require"
)

func TestNewIrreversibleBlocksIndex(t *testing.T) {
	tests := []struct {
		name                      string
		irreversibleBlocksIndexes map[int]map[int]map[int]string
		startBlockNum             uint64
		cursorBlockRef            BlockRef
		expectNil                 bool
		expectNextBlockIDs        []string
		expectLoadedUpper         uint64
	}{
		{
			name: "sunny",
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			startBlockNum:      0,
			cursorBlockRef:     nil,
			expectNil:          false,
			expectNextBlockIDs: []string{"4a", "6a"},
			expectLoadedUpper:  99,
		},
		{
			"sunny reads bigger index",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
				1000: {
					0: {
						4:   "4a",
						6:   "6a",
						210: "210a",
					},
				},
			},
			0,
			nil,
			false,
			[]string{"4a", "6a", "210a"},
			999,
		},

		{
			"skip to start block",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			5,
			nil,
			false,
			[]string{"6a"},
			99,
		},
		{
			"skip to next index on start block",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
					100: {
						104: "104a",
						106: "106a",
					},
				},
			},
			106,
			nil,
			false,
			[]string{"106a"},
			199,
		},
		{
			"matching cursorBlockref",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			4,
			BasicBlockRef{"4a", 4},
			false,
			[]string{"4a", "6a"},
			99,
		},
		{
			"non-matching cursorBlockref",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			4,
			BasicBlockRef{"4b", 4},
			true,
			nil,
			0,
		},
		{
			"no index up to this height",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			104,
			nil,
			true,
			nil,
			0,
		},
		{
			"no index at all",
			map[int]map[int]map[int]string{
				100: {},
			},
			1,
			nil,
			true,
			nil,
			0,
		},
		{
			"empty index",
			map[int]map[int]map[int]string{
				100: {
					0: {},
				},
			},
			1,
			nil,
			false,
			nil,
			99,
		},
		{
			name: "no data until later",
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {},
					100: {
						104: "104a",
						106: "106a",
					},
				},
			},
			startBlockNum:      1,
			cursorBlockRef:     nil,
			expectNil:          false,
			expectNextBlockIDs: []string{"104a", "106a"},
			expectLoadedUpper:  199,
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			if c.name != "no data until later" {
				return
			}

			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)

			bi := NewBlockIndexesManager(context.Background(), irrStore, bundleSizes, c.startBlockNum, 9999999, c.cursorBlockRef, nil)

			if c.expectNil {
				require.Nil(t, bi)
				return
			}
			require.NotNil(t, bi)
			var nextBlockIDs []string
			for _, br := range bi.nextBlockRefs {
				nextBlockIDs = append(nextBlockIDs, br.BlockID)
			}
			assert.Equal(t, c.expectNextBlockIDs, nextBlockIDs)
			assert.Equal(t, c.expectLoadedUpper, bi.irrIdxLoadedUpperBoundary)

		})
	}

}

func TestIrreversibleBlocksIndexNextMergedBlocksBase(t *testing.T) {

	type expected struct {
		BaseNum  uint64
		LIB      BlockRef
		HasIndex bool
	}

	tests := []struct {
		name                         string
		nextBlockRefs                []*pb.BlockRef
		loadedUpperBoundary          uint64
		loadedUpperIrreversibleBlock *pb.BlockRef

		expected expected
	}{
		{
			"low blocks wanted",
			[]*pb.BlockRef{
				pbBlockRef("4a", 4),
				pbBlockRef("6a", 6),
			},
			99,
			pbBlockRef("99a", 99),
			expected{0, nil, true}, // starting a filesource at 0 will traverse any further blocksfile (0, 100, 200...) so we always say 0 even if you've read it already
		},
		{
			"high blocks wanted",
			[]*pb.BlockRef{
				pbBlockRef("104a", 104),
				pbBlockRef("106a", 106),
			},
			199,
			pbBlockRef("199a", 199),
			expected{100, nil, true},
		},
		{
			"no more blocks, or no blocks ever",
			[]*pb.BlockRef{},
			199,
			pbBlockRef("199a", 199),
			expected{200, BasicBlockRef{"199a", 199}, false},
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			bi := &BlockIndexesManager{
				nextBlockRefs:                c.nextBlockRefs,
				irrIdxLoadedUpperBoundary:    c.loadedUpperBoundary,
				loadedUpperIrreversibleBlock: c.loadedUpperIrreversibleBlock,
			}

			base, lib, hasIndex := bi.NextMergedBlocksBase()
			assert.EqualValues(t, c.expected.BaseNum, base)
			assert.Equal(t, c.expected.LIB, lib)
			assert.Equal(t, c.expected.HasIndex, hasIndex)

		})
	}

}

func TestIrreversibleBlocksLoadRangesUntil(t *testing.T) {
	type expected struct {
		noMoreIndexes       bool
		nextBlockRefs       []*pb.BlockRef
		loadedUpperBoundary uint64
	}
	tests := []struct {
		name                      string
		irreversibleBlocksIndexes map[int]map[int]map[int]string
		noMoreIndexes             bool
		nextBlockRefs             []*pb.BlockRef
		loadedUpperBoundary       uint64
		untilWhat                 uint64
		expected                  expected
	}{
		{
			name: "sunny",
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				1000: {
					0:    {4: "4a", 6: "6a"},
					1000: {1004: "1004a", 1006: "1006a"},
				},
			},
			noMoreIndexes:       false,
			nextBlockRefs:       nil, //[]*pb.BlockRef{pbBlockRef("6a", 6)},
			loadedUpperBoundary: 999,
			untilWhat:           0,
			expected: expected{
				false,
				[]*pb.BlockRef{pbBlockRef("1004a", 1004), pbBlockRef("1006a", 1006)},
				1999,
			},
		},
		{
			"no need to load",
			map[int]map[int]map[int]string{
				1000: {
					0:    {4: "4a", 6: "6a"},
					1000: {1004: "1004a", 1006: "1006a"},
				},
			},
			false,
			[]*pb.BlockRef{pbBlockRef("6a", 6)},
			999,
			0,
			expected{
				false,
				[]*pb.BlockRef{pbBlockRef("6a", 6)},
				999,
			},
		},
		{
			name: "nothing more to load",
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				1000: {
					0: {4: "4a", 6: "6a"},
				},
			},
			noMoreIndexes:       false,
			nextBlockRefs:       nil,
			loadedUpperBoundary: 999,
			untilWhat:           0,
			expected: expected{
				noMoreIndexes:       true,
				nextBlockRefs:       nil,
				loadedUpperBoundary: 999,
			},
		},
		{
			"load until next",
			map[int]map[int]map[int]string{
				100: {
					0:   {4: "4a", 6: "6a"},
					100: {},
					200: {},
					300: {305: "305a", 309: "309a"},
				},
			},
			false,
			nil,
			99,
			0,
			expected{
				false,
				[]*pb.BlockRef{pbBlockRef("305a", 305), pbBlockRef("309a", 309)},
				399,
			},
		},
		{
			"load until specific",
			map[int]map[int]map[int]string{
				100: {
					0:   {4: "4a", 6: "6a"},
					100: {104: "104a"},
					200: {207: "207a"},
					300: {305: "305a", 309: "309a"},
				},
			},
			false,
			[]*pb.BlockRef{pbBlockRef("6a", 6)},
			99,
			310,
			expected{
				false,
				[]*pb.BlockRef{
					pbBlockRef("6a", 6),
					pbBlockRef("104a", 104),
					pbBlockRef("207a", 207),
					pbBlockRef("305a", 305),
					pbBlockRef("309a", 309),
				},
				399,
			},
		},
		{
			"load passed end",
			map[int]map[int]map[int]string{
				100: {
					0:   {4: "4a", 6: "6a"},
					100: {104: "104a"},
					200: {207: "207a"},
				},
			},
			false,
			[]*pb.BlockRef{pbBlockRef("6a", 6)},
			99,
			310,
			expected{
				true,
				[]*pb.BlockRef{
					pbBlockRef("6a", 6),
					pbBlockRef("104a", 104),
					pbBlockRef("207a", 207),
				},
				299,
			},
		},
		{
			"don't load when already ended",
			map[int]map[int]map[int]string{
				100: {
					0:   {4: "4a", 6: "6a"},
					100: {104: "104a"},
				},
			},
			true,
			[]*pb.BlockRef{pbBlockRef("6a", 6)},
			99,
			104,
			expected{
				true,
				[]*pb.BlockRef{
					pbBlockRef("6a", 6),
				},
				99,
			},
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)

			bi := &BlockIndexesManager{
				noMoreIrrIdx:              c.noMoreIndexes,
				irrIdxLoadedUpperBoundary: c.loadedUpperBoundary,
				nextBlockRefs:             c.nextBlockRefs,
				irrIdxStore:               irrStore,
				irrIdxPossibleSizes:       bundleSizes,
				ctx:                       context.Background(),
			}

			if c.untilWhat == 0 {
				bi.loadRangesUntilMatch()
			} else {
				bi.loadRangesUntil(c.untilWhat)
			}

			assert.EqualValues(t, c.expected.loadedUpperBoundary, bi.irrIdxLoadedUpperBoundary)
			assert.Equal(t, c.expected.nextBlockRefs, bi.nextBlockRefs)
			assert.Equal(t, c.expected.noMoreIndexes, bi.noMoreIrrIdx)

		})
	}

}

func TestIrreversibleBlocksSkip(t *testing.T) {
	tests := []struct {
		name                string
		nextBlockRefs       []*pb.BlockRef
		loadedUpperBoundary uint64

		irreversibleBlocksIndexes map[int]map[int]map[int]string
		skipWhat                  BlockRef
		expectSkip                bool
	}{
		{
			"sunny skip",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			map[int]map[int]map[int]string{100: {}},
			BasicBlockRef{"6b", 6},
			true,
		},
		{
			"sunny noskip",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			map[int]map[int]map[int]string{100: {}},
			BasicBlockRef{"6a", 6},
			false,
		},
		{
			"further, load and noskip",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			map[int]map[int]map[int]string{100: {
				0:   {6: "6a", 8: "8a"},
				100: {104: "104a", 106: "106a"},
			}},
			BasicBlockRef{"104a", 104},
			false,
		},
		{
			"further no index, skip",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			map[int]map[int]map[int]string{100: {}},
			BasicBlockRef{"104a", 104},
			true,
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)

			bi := &BlockIndexesManager{
				//noMoreIndexes:       c.noMoreIndexes,
				irrIdxLoadedUpperBoundary: c.loadedUpperBoundary,
				nextBlockRefs:             c.nextBlockRefs,
				irrIdxStore:               irrStore,
				irrIdxPossibleSizes:       bundleSizes,
				ctx:                       context.Background(),
			}

			seenSkip := bi.Skip(c.skipWhat)

			assert.Equal(t, c.expectSkip, seenSkip)

		})
	}

}

func TestIrreversibleBlocksReorder(t *testing.T) {

	var emptyDisorderedMap = map[uint64]*PreprocessedBlock{}

	type expected struct {
		out                       []*PreprocessedBlock
		noMoreIndex               bool
		nextBlockRefs             []*pb.BlockRef
		pendingPreprocessedBlocks map[uint64]*PreprocessedBlock
		lastProcessedBlock        *PreprocessedBlock
	}
	tests := []struct {
		name                      string
		nextBlockRefs             []*pb.BlockRef
		loadedUpperBoundary       uint64
		pendingPreprocessedBlocks map[uint64]*PreprocessedBlock
		reorderWhat               *PreprocessedBlock
		expected                  expected
	}{
		{
			"sunny",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			emptyDisorderedMap,
			ppBlk("6a", 6),
			expected{
				[]*PreprocessedBlock{ppBlk("6a", 6)},
				false,
				[]*pb.BlockRef{
					pbBlockRef("8a", 8),
				},
				emptyDisorderedMap,
				ppBlk("6a", 6),
			},
		},
		{
			"awaiting disordered",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("10a", 10),
			},
			99,
			map[uint64]*PreprocessedBlock{
				8: ppBlk("8a", 8),
				9: ppBlk("9a", 9),
			},
			ppBlk("6a", 6),
			expected{
				[]*PreprocessedBlock{
					ppBlk("6a", 6),
					ppBlk("8a", 8),
					ppBlk("9a", 9),
				},
				false,
				[]*pb.BlockRef{
					pbBlockRef("10a", 10),
				},
				emptyDisorderedMap,
				ppBlk("9a", 9),
			},
		},
		{
			"go to disordered",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("10a", 10),
			},
			99,
			map[uint64]*PreprocessedBlock{8: ppBlk("8a", 8)},
			ppBlk("10a", 10), // send 10a instead of expected 6a
			expected{
				nil,
				false,
				[]*pb.BlockRef{
					pbBlockRef("6a", 6), // 6a still expected
				},
				map[uint64]*PreprocessedBlock{
					8:  ppBlk("8a", 8),
					10: ppBlk("10a", 10), // 10a moved to disordered
				},
				nil,
			},
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			bi := &BlockIndexesManager{
				irrIdxLoadedUpperBoundary: c.loadedUpperBoundary,
				nextBlockRefs:             c.nextBlockRefs,
				pendingPreprocessedBlocks: c.pendingPreprocessedBlocks,
			}

			var out []*PreprocessedBlock
			h := HandlerFunc(func(blk *Block, obj interface{}) error {
				out = append(out, &PreprocessedBlock{
					blk,
					obj,
				})
				return nil
			})

			lastProcessedBlock, noMoreIndex, err := bi.ProcessOrderedSegment(c.reorderWhat, h)

			require.NoError(t, err)

			if c.expected.lastProcessedBlock == nil {
				assert.Nil(t, lastProcessedBlock)
			} else {
				assert.Equal(t, c.expected.lastProcessedBlock.Block, lastProcessedBlock.Block)
			}
			assert.Equal(t, c.expected.out, out)
			assert.Equal(t, c.expected.noMoreIndex, noMoreIndex)
			assert.Equal(t, c.expected.pendingPreprocessedBlocks, bi.pendingPreprocessedBlocks)
			assert.Equal(t, c.expected.nextBlockRefs, bi.nextBlockRefs)

		})
	}

}

func TestWithinIndexRange(t *testing.T) {
	tests := []struct {
		name                string
		stopBlockNum        uint64
		loadedUpperBoundary uint64
		assertInRange       []uint64
		assertOutsideRange  []uint64
	}{
		{
			"static no call to loadRange",
			1995,
			2000,
			[]uint64{10, 1000, 1500},
			[]uint64{1996, 2000},
		},
		{
			"with dummy call to loadrange",
			3000,
			999,
			[]uint64{},
			[]uint64{2100, 2800},
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			bi := &BlockIndexesManager{
				irrIdxLoadedUpperBoundary: c.loadedUpperBoundary,
				stopBlockNum:              c.stopBlockNum,
			}
			for _, tr := range c.assertInRange {
				assert.True(t, bi.withinIndexRange(tr))
			}
			for _, tr := range c.assertOutsideRange {
				assert.False(t, bi.withinIndexRange(tr))
			}

		})
	}

}

func getIrrStore(irrBlkIdxs map[int]map[int]map[int]string) (irrStore *dstore.MockStore, bundleSizes []uint64) {
	irrStore = dstore.NewMockStore(nil)
	for i, m := range irrBlkIdxs {
		bundleSizes = append(bundleSizes, uint64(i))
		for j, n := range m {
			filename, cnt := TestIrrBlocksIdx(j, i, n)
			irrStore.SetFile(filename, cnt)
		}

	}
	sort.Slice(bundleSizes, func(i, j int) bool { return bundleSizes[i] > bundleSizes[j] })
	return
}

func pbBlockRef(id string, num uint64) *pb.BlockRef {
	return &pb.BlockRef{
		BlockNum: num,
		BlockID:  id,
	}
}

func ppBlk(id string, num uint64) *PreprocessedBlock {
	return &PreprocessedBlock{
		Block: &Block{Id: id, Number: num},
	}
}
