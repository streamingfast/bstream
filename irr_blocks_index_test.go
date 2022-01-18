package bstream

import (
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
			"sunny",
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			0,
			nil,
			false,
			[]string{"4a", "6a"},
			99,
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
			"no data until later",
			map[int]map[int]map[int]string{
				100: {
					0: {},
					100: {
						104: "104a",
						106: "106a",
					},
				},
			},
			1,
			nil,
			false,
			[]string{"104a", "106a"},
			199,
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)

			bi := newIrreversibleBlocksIndex(irrStore, bundleSizes, c.startBlockNum, c.cursorBlockRef)

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
			assert.Equal(t, c.expectLoadedUpper, bi.loadedUpperBoundary)

		})
	}

}

func TestIrreversibleBlocksIndexNextBaseBlock(t *testing.T) {

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
			expected{0, nil, true},
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

			bi := &irrBlocksIndex{
				nextBlockRefs:                c.nextBlockRefs,
				loadedUpperBoundary:          c.loadedUpperBoundary,
				loadedUpperIrreversibleBlock: c.loadedUpperIrreversibleBlock,
			}

			base, lib, hasIndex := bi.NextBaseBlock()
			assert.EqualValues(t, c.expected.BaseNum, base)
			assert.Equal(t, c.expected.LIB, lib)
			assert.Equal(t, c.expected.HasIndex, hasIndex)

			//expectedBlockCount := len(c.expectedBlockIDs)

			//var receivedBlockIDs []string

			//testDone := make(chan interface{})
			//handler := HandlerFunc(func(blk *Block, obj interface{}) error {
			//	receivedBlockIDs = append(receivedBlockIDs, blk.Id)
			//	if len(receivedBlockIDs) == expectedBlockCount {
			//		close(testDone)
			//	}
			//	return nil
			//})

			//ifs := &IndexedFileSource{
			//	//bs, startBlockNum, 1, nil, handler, FileSourceWithSkipForkedBlocks(irrStore, c.bundleSizes, mustMatch)
			//	logger:            zlog,
			//	handler:           handler,
			//	blockIndex:        newIrreversibleBlocksIndex(irrStore, c.bundleSizes, startBlockNum, mustMatch),
			//	blockStore:        bs,
			//	nextSourceFactory: nil, //FIXME
			//	preprocFunc: func(blk *Block) (interface{}, error) {
			//		fmt.Println("preprocfunc called")
			//		return nil, nil
			//	},
			//}
			//go ifs.run()

			//select {
			//case <-testDone:
			//case <-time.After(100 * time.Millisecond):
			//	t.Error(fmt.Sprintf("Test timeout waiting for %d blocks", expectedBlockCount))
			//}

			//assert.EqualValues(t, c.expectedBlockIDs, receivedBlockIDs)
			////			ifs.Shutdown(nil)

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
			"sunny",
			map[int]map[int]map[int]string{
				1000: {
					0:    {4: "4a", 6: "6a"},
					1000: {1004: "1004a", 1006: "1006a"},
				},
			},
			false,
			nil, //[]*pb.BlockRef{pbBlockRef("6a", 6)},
			999,
			0,
			expected{
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
			"nothing more to load",
			map[int]map[int]map[int]string{
				1000: {
					0: {4: "4a", 6: "6a"},
				},
			},
			false,
			nil,
			999,
			0,
			expected{
				true,
				nil,
				999,
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

			bi := &irrBlocksIndex{
				noMoreIndexes:       c.noMoreIndexes,
				loadedUpperBoundary: c.loadedUpperBoundary,
				nextBlockRefs:       c.nextBlockRefs,
				store:               irrStore,
				bundleSizes:         bundleSizes,
			}

			bi.loadRangesUntil(c.untilWhat)

			assert.EqualValues(t, c.expected.loadedUpperBoundary, bi.loadedUpperBoundary)
			assert.Equal(t, c.expected.nextBlockRefs, bi.nextBlockRefs)
			assert.Equal(t, c.expected.noMoreIndexes, bi.noMoreIndexes)

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
			"further noskip",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			map[int]map[int]map[int]string{100: {
				100: {
					104: "104a",
					106: "106a",
				},
			}},
			BasicBlockRef{"104a", 104},
			false,
		},
		{
			"further no index, noskip",
			[]*pb.BlockRef{
				pbBlockRef("6a", 6),
				pbBlockRef("8a", 8),
			},
			99,
			map[int]map[int]map[int]string{100: {}},
			BasicBlockRef{"104a", 104},
			false,
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)

			bi := &irrBlocksIndex{
				//noMoreIndexes:       c.noMoreIndexes,
				loadedUpperBoundary: c.loadedUpperBoundary,
				nextBlockRefs:       c.nextBlockRefs,
				store:               irrStore,
				bundleSizes:         bundleSizes,
			}

			seenSkip := bi.Skip(c.skipWhat)

			assert.Equal(t, c.expectSkip, seenSkip)

		})
	}

}

func TestIrreversibleBlocksReorder(t *testing.T) {

	var emptyDisorderedMap = map[uint64]*PreprocessedBlock{}

	type expected struct {
		out           []*PreprocessedBlock
		nextBlockRefs []*pb.BlockRef
		disordered    map[uint64]*PreprocessedBlock
	}
	tests := []struct {
		name                string
		nextBlockRefs       []*pb.BlockRef
		loadedUpperBoundary uint64
		disordered          map[uint64]*PreprocessedBlock

		reorderWhat *PreprocessedBlock
		expected    expected
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
				[]*pb.BlockRef{
					pbBlockRef("8a", 8),
				},
				emptyDisorderedMap,
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
				[]*pb.BlockRef{
					pbBlockRef("10a", 10),
				},
				emptyDisorderedMap,
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
			ppBlk("10a", 10),
			expected{
				nil,
				[]*pb.BlockRef{
					pbBlockRef("10a", 10),
				},
				map[uint64]*PreprocessedBlock{
					8:  ppBlk("8a", 8),
					10: ppBlk("10a", 10),
				},
			},
		},
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			bi := &irrBlocksIndex{
				loadedUpperBoundary: c.loadedUpperBoundary,
				nextBlockRefs:       c.nextBlockRefs,
				disordered:          c.disordered,
			}

			out := bi.Reorder(c.reorderWhat)

			assert.Equal(t, c.expected.out, out)
			//assert.Equal(t, c.expected.disordered, bi.disordered)
			//assert.Equal(t, c.expected.nextBlockRefs, bi.nextBlockRefs)

		})
	}

}

//func TestIrreversibleBlocksIndexSkip(t *testing.T) {
//	tests := []struct {
//		name                      string
//		irreversibleBlocksIndexes map[int]map[int]map[int]string
//		startBlockNum             uint64
//		cursorBlockRef            BlockRef
//		expectNil                 bool
//		expectNextBlockIDs        []string
//		expectLoadedUpper         uint64
//	}{
//		{
//			"sunny",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//			},
//			0,
//			nil,
//			false,
//			[]string{"4a", "6a"},
//			99,
//		},
//		{
//			"sunny reads bigger index",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//				1000: {
//					0: {
//						4:   "4a",
//						6:   "6a",
//						210: "210a",
//					},
//				},
//			},
//			0,
//			nil,
//			false,
//			[]string{"4a", "6a", "210a"},
//			999,
//		},
//
//		{
//			"skip to start block",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//			},
//			5,
//			nil,
//			false,
//			[]string{"6a"},
//			99,
//		},
//		{
//			"skip to next index on start block",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//					100: {
//						104: "104a",
//						106: "106a",
//					},
//				},
//			},
//			106,
//			nil,
//			false,
//			[]string{"106a"},
//			199,
//		},
//		{
//			"matching cursorBlockref",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//			},
//			4,
//			BasicBlockRef{"4a", 4},
//			false,
//			[]string{"4a", "6a"},
//			99,
//		},
//		{
//			"non-matching cursorBlockref",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//			},
//			4,
//			BasicBlockRef{"4b", 4},
//			true,
//			nil,
//			0,
//		},
//		{
//			"no index up to this height",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//			},
//			104,
//			nil,
//			true,
//			nil,
//			0,
//		},
//		{
//			"no index at all",
//			map[int]map[int]map[int]string{
//				100: {},
//			},
//			1,
//			nil,
//			true,
//			nil,
//			0,
//		},
//		{
//			"empty index",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {},
//				},
//			},
//			1,
//			nil,
//			false,
//			nil,
//			99,
//		},
//		{
//			"no data until later",
//			map[int]map[int]map[int]string{
//				100: {
//					0: {},
//					100: {
//						104: "104a",
//						106: "106a",
//					},
//				},
//			},
//			1,
//			nil,
//			false,
//			[]string{"104a", "106a"},
//			199,
//		},
//	}
//
//	for _, c := range tests {
//		t.Run(c.name, func(t *testing.T) {
//
//			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)
//
//			bi := newIrreversibleBlocksIndex(irrStore, bundleSizes, c.startBlockNum, c.cursorBlockRef)
//
//			if c.expectNil {
//				require.Nil(t, bi)
//				return
//			}
//			require.NotNil(t, bi)
//			var nextBlockIDs []string
//			for _, br := range bi.nextBlockRefs {
//				nextBlockIDs = append(nextBlockIDs, br.BlockID)
//			}
//			assert.Equal(t, c.expectNextBlockIDs, nextBlockIDs)
//			assert.Equal(t, c.expectLoadedUpper, bi.loadedUpperBoundary)
//
//		})
//	}
//
//}

//
////func TestIrreversibleBlocksIndex(t *testing.T) {
////
////	tests := []struct {
////		name                      string
////		files                     map[int][]byte
////		irreversibleBlocksIndexes map[int]map[int]map[int]string
////		expectedBlockIDs          []string
////		bundleSizes               []uint64
////		cursorBlockRef            BlockRef
////	}{
////		{
////			"skip forked blocks",
////			map[int][]byte{0: testBlocks(
////				4, "4a", "3a", 0,
////				6, "6a", "4a", 0,
////				6, "6b", "4a", 0,
////				99, "99a", "6a", 0,
////			),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0: {
////						4: "4a",
////						6: "6a",
////					},
////				},
////			},
////			[]string{
////				"4a",
////				"6a",
////			},
////			[]uint64{100},
////			nil,
////		},
////		{
////			"send everything if no index",
////			map[int][]byte{0: testBlocks(
////				4, "4a", "3a", 0,
////				6, "6a", "4a", 0,
////				6, "6b", "4a", 0,
////				99, "99a", "6a", 0,
////			),
////			},
////			nil,
////			[]string{
////				"4a",
////				"6a",
////				"6b",
////				"99a",
////			},
////			[]uint64{100},
////			nil,
////		},
////		{
////			"skip everything if empty index",
////			map[int][]byte{
////				0: testBlocks(
////					4, "4a", "3a", 0,
////					6, "6a", "4a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "zz", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0:   {},
////					100: {100: "100a"},
////				},
////			},
////			[]string{"100a"},
////			[]uint64{100},
////			nil,
////		},
////		{
////			"transition to unindexed range",
////			map[int][]byte{
////				0: testBlocks(
////					4, "4a", "zz", 0,
////					6, "6a", "4a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "6a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0: {4: "4a", 6: "6a"},
////				},
////			},
////			[]string{"4a", "6a", "100a"},
////			[]uint64{100},
////			nil,
////		},
////		{
////			"never transition back to indexed range",
////			map[int][]byte{
////				0: testBlocks(
////					4, "4a", "zz", 0,
////					6, "6a", "4a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "6a", 0,
////					100, "100b", "6a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					100: {100: "100a"},
////				},
////			},
////			[]string{"4a", "6a", "100a", "100b"},
////			[]uint64{100},
////			nil,
////		},
////		{
////			"irreversible blocks in next block file",
////			map[int][]byte{
////				0: testBlocks(
////					1, "1a", "0a", 0,
////					50, "50a", "1a", 0,
////					75, "75b", "50a", 0,
////				),
////				100: testBlocks(
////					75, "75a", "50a", 0, // this DOES happen with forky chains
////					100, "100a", "75a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0:   {1: "1a", 50: "50a", 75: "75a"},
////					100: {100: "100a"},
////				},
////			},
////			[]string{"1a", "50a", "75a", "100a"},
////			[]uint64{100},
////			nil,
////		},
////		{
////			"large file takes precedence over small files",
////			map[int][]byte{
////				0: testBlocks(
////					1, "1a", "0a", 0,
////					50, "50a", "1a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "50a", 0,
////				),
////				200: testBlocks(
////					200, "200a", "50a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0:   {1: "1a", 50: "50a"},
////					100: {100: "NONEXISTING"}, // this is not a real life scenario, only for testing
////				},
////				1000: {
////					0: {1: "1a", 50: "50a", 200: "200a"},
////				},
////			},
////			[]string{"1a", "50a", "200a"},
////			[]uint64{100, 1000},
////			nil,
////		},
////		{
////			"don't use index if cursor is on a forked block",
////			map[int][]byte{
////				0: testBlocks(
////					2, "2a", "1a", 0,
////					2, "2b", "1a", 0,
////					3, "3a", "2a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "3a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0:   {2: "2a", 3: "3a"},
////					100: {100: "100a"},
////				},
////			},
////			[]string{"2a", "2b", "3a", "100a"},
////			[]uint64{100},
////			BasicBlockRef{"2b", 2},
////		},
////		{
////			"use index if cursor is on a valid block",
////			map[int][]byte{
////				0: testBlocks(
////					2, "2a", "1a", 0,
////					2, "2b", "1a", 0,
////					3, "3a", "2a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "3a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0:   {2: "2a", 3: "3a"},
////					100: {100: "100a"},
////				},
////			},
////			[]string{"2a", "3a", "100a"},
////			[]uint64{100},
////			BasicBlockRef{"2a", 2},
////		},
////		{
////			"start after first",
////			map[int][]byte{
////				0: testBlocks(
////					2, "2a", "1a", 0,
////					2, "2b", "1a", 0,
////					3, "3a", "2a", 0,
////				),
////				100: testBlocks(
////					100, "100a", "3a", 0,
////				),
////			},
////			map[int]map[int]map[int]string{
////				100: {
////					0:   {2: "2a", 3: "3a"},
////					100: {100: "100a"},
////				},
////			},
////			[]string{"2a", "3a", "100a"},
////			[]uint64{100},
////			BasicBlockRef{"100a", 100},
////		},
////	}
////
////	for _, c := range tests {
////		t.Run(c.name, func(t *testing.T) {
////
////			bs := dstore.NewMockStore(nil)
////			for i, data := range c.files {
////				bs.SetFile(base(i), data)
////			}
////
////			irrStore := dstore.NewMockStore(nil)
////			for i, m := range c.irreversibleBlocksIndexes {
////				for j, n := range m {
////					filename, cnt := testIrrBlocksIdx(j, i, n)
////					irrStore.SetFile(filename, cnt)
////				}
////
////			}
////
////			startBlockNum := uint64(1)
////			if c.cursorBlockRef != nil {
////				startBlockNum = c.cursorBlockRef.Num()
////			}
////
////			var mustMatch BlockRef
////			if c.cursorBlockRef != nil {
////				mustMatch = c.cursorBlockRef
////			}
////
////			bi := newIrreversibleBlocksIndex(irrStore, c.bundleSizes, startBlockNum, mustMatch)
////
////			var lastSeen BlockRef
////			x, y, z := bi.NextRange(lastSeen)
////			assert.EqualValues(t, x, 0)
////			assert.Equal(t, y, nil)
////			assert.Equal(t, z, true)
////
////			//expectedBlockCount := len(c.expectedBlockIDs)
////
////			//var receivedBlockIDs []string
////
////			//testDone := make(chan interface{})
////			//handler := HandlerFunc(func(blk *Block, obj interface{}) error {
////			//	receivedBlockIDs = append(receivedBlockIDs, blk.Id)
////			//	if len(receivedBlockIDs) == expectedBlockCount {
////			//		close(testDone)
////			//	}
////			//	return nil
////			//})
////
////			//ifs := &IndexedFileSource{
////			//	//bs, startBlockNum, 1, nil, handler, FileSourceWithSkipForkedBlocks(irrStore, c.bundleSizes, mustMatch)
////			//	logger:            zlog,
////			//	handler:           handler,
////			//	blockIndex:        newIrreversibleBlocksIndex(irrStore, c.bundleSizes, startBlockNum, mustMatch),
////			//	blockStore:        bs,
////			//	nextSourceFactory: nil, //FIXME
////			//	preprocFunc: func(blk *Block) (interface{}, error) {
////			//		fmt.Println("preprocfunc called")
////			//		return nil, nil
////			//	},
////			//}
////			//go ifs.run()
////
////			//select {
////			//case <-testDone:
////			//case <-time.After(100 * time.Millisecond):
////			//	t.Error(fmt.Sprintf("Test timeout waiting for %d blocks", expectedBlockCount))
////			//}
////
////			//assert.EqualValues(t, c.expectedBlockIDs, receivedBlockIDs)
////			////			ifs.Shutdown(nil)
////
////		})
////	}
////
////}

func getIrrStore(irrBlkIdxs map[int]map[int]map[int]string) (irrStore *dstore.MockStore, bundleSizes []uint64) {
	irrStore = dstore.NewMockStore(nil)
	for i, m := range irrBlkIdxs {
		bundleSizes = append(bundleSizes, uint64(i))
		for j, n := range m {
			filename, cnt := testIrrBlocksIdx(j, i, n)
			irrStore.SetFile(filename, cnt)
		}

	}
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
