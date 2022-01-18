package bstream

//import (
//	"fmt"
//	"testing"
//	"time"
//
//	"github.com/streamingfast/dstore"
//	"github.com/stretchr/testify/assert"
//)
//
//func TestFileSource_IrrIndex(t *testing.T) {
//
//	tests := []struct {
//		name                      string
//		files                     map[int][]byte
//		irreversibleBlocksIndexes map[int]map[int]map[int]string
//		expectedBlockIDs          []string
//		bundleSizes               []uint64
//		cursorBlockRef            BlockRef
//	}{
//		{
//			"skip forked blocks",
//			map[int][]byte{0: testBlocks(
//				4, "4a", "3a", 0,
//				6, "6a", "4a", 0,
//				6, "6b", "4a", 0,
//				99, "99a", "6a", 0,
//			),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0: {
//						4: "4a",
//						6: "6a",
//					},
//				},
//			},
//			[]string{
//				"4a",
//				"6a",
//			},
//			[]uint64{100},
//			nil,
//		},
//		{
//			"send everything if no index",
//			map[int][]byte{0: testBlocks(
//				4, "4a", "3a", 0,
//				6, "6a", "4a", 0,
//				6, "6b", "4a", 0,
//				99, "99a", "6a", 0,
//			),
//			},
//			nil,
//			[]string{
//				"4a",
//				"6a",
//				"6b",
//				"99a",
//			},
//			[]uint64{100},
//			nil,
//		},
//		{
//			"skip everything if empty index",
//			map[int][]byte{
//				0: testBlocks(
//					4, "4a", "3a", 0,
//					6, "6a", "4a", 0,
//				),
//				100: testBlocks(
//					100, "100a", "zz", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0:   {},
//					100: {100: "100a"},
//				},
//			},
//			[]string{"100a"},
//			[]uint64{100},
//			nil,
//		},
//		{
//			"transition to unindexed range",
//			map[int][]byte{
//				0: testBlocks(
//					4, "4a", "zz", 0,
//					6, "6a", "4a", 0,
//				),
//				100: testBlocks(
//					100, "100a", "6a", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0: {4: "4a", 6: "6a"},
//				},
//			},
//			[]string{"4a", "6a", "100a"},
//			[]uint64{100},
//			nil,
//		},
//		{
//			"never transition back to indexed range",
//			map[int][]byte{
//				0: testBlocks(
//					4, "4a", "zz", 0,
//					6, "6a", "4a", 0,
//				),
//				100: testBlocks(
//					100, "100a", "6a", 0,
//					100, "100b", "6a", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					100: {100: "100a"},
//				},
//			},
//			[]string{"4a", "6a", "100a", "100b"},
//			[]uint64{100},
//			nil,
//		},
//		{
//			"irreversible blocks in next block file",
//			map[int][]byte{
//				0: testBlocks(
//					1, "1a", "0a", 0,
//					50, "50a", "1a", 0,
//					75, "75b", "50a", 0,
//				),
//				100: testBlocks(
//					75, "75a", "50a", 0, // this DOES happen with forky chains
//					100, "100a", "75a", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0:   {1: "1a", 50: "50a", 75: "75a"},
//					100: {100: "100a"},
//				},
//			},
//			[]string{"1a", "50a", "75a", "100a"},
//			[]uint64{100},
//			nil,
//		},
//		{
//			"large file takes precedence over small files",
//			map[int][]byte{
//				0: testBlocks(
//					1, "1a", "0a", 0,
//					50, "50a", "1a", 0,
//				),
//				100: testBlocks(
//					100, "100a", "50a", 0,
//				),
//				200: testBlocks(
//					200, "200a", "50a", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0:   {1: "1a", 50: "50a"},
//					100: {100: "NONEXISTING"}, // this is not a real life scenario, only for testing
//				},
//				1000: {
//					0: {1: "1a", 50: "50a", 200: "200a"},
//				},
//			},
//			[]string{"1a", "50a", "200a"},
//			[]uint64{100, 1000},
//			nil,
//		},
//		{
//			"don't use index if cursor is on a forked block",
//			map[int][]byte{
//				0: testBlocks(
//					2, "2a", "1a", 0,
//					2, "2b", "1a", 0,
//					3, "3a", "2a", 0,
//				),
//				100: testBlocks(
//					100, "100a", "3a", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0:   {2: "2a", 3: "3a"},
//					100: {100: "100a"},
//				},
//			},
//			[]string{"2a", "2b", "3a", "100a"},
//			[]uint64{100},
//			BasicBlockRef{"2b", 2},
//		},
//		{
//			"use index if cursor is on a valid block",
//			map[int][]byte{
//				0: testBlocks(
//					2, "2a", "1a", 0,
//					2, "2b", "1a", 0,
//					3, "3a", "2a", 0,
//				),
//				100: testBlocks(
//					100, "100a", "3a", 0,
//				),
//			},
//			map[int]map[int]map[int]string{
//				100: {
//					0:   {2: "2a", 3: "3a"},
//					100: {100: "100a"},
//				},
//			},
//			[]string{"2a", "3a", "100a"},
//			[]uint64{100},
//			BasicBlockRef{"2a", 2},
//		},
//	}
//
//	for _, c := range tests {
//		t.Run(c.name, func(t *testing.T) {
//
//			bs := dstore.NewMockStore(nil)
//			for i, data := range c.files {
//				bs.SetFile(base(i), data)
//			}
//
//			irrStore := dstore.NewMockStore(nil)
//
//			for i, m := range c.irreversibleBlocksIndexes {
//				for j, n := range m {
//					filename, cnt := testIrrBlocksIdx(j, i, n)
//					irrStore.SetFile(filename, cnt)
//				}
//
//			}
//			expectedBlockCount := len(c.expectedBlockIDs)
//
//			var receivedBlockIDs []string
//
//			testDone := make(chan interface{})
//			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
//				receivedBlockIDs = append(receivedBlockIDs, blk.Id)
//				if len(receivedBlockIDs) == expectedBlockCount {
//					close(testDone)
//				}
//				return nil
//			})
//
//			startBlockNum := uint64(1)
//			if c.cursorBlockRef != nil {
//				startBlockNum = c.cursorBlockRef.Num()
//			}
//			var mustMatch BlockRef
//			if c.cursorBlockRef != nil {
//				mustMatch = c.cursorBlockRef
//			}
//			ifs := &IndexedFileSource{
//				//bs, startBlockNum, 1, nil, handler, FileSourceWithSkipForkedBlocks(irrStore, c.bundleSizes, mustMatch)
//				logger:            zlog,
//				handler:           handler,
//				blockIndex:        newIrreversibleBlocksIndex(irrStore, c.bundleSizes, startBlockNum, mustMatch),
//				blockStore:        bs,
//				nextSourceFactory: nil, //FIXME
//				preprocFunc: func(blk *Block) (interface{}, error) {
//					fmt.Println("preprocfunc called")
//					return nil, nil
//				},
//			}
//			go ifs.run()
//
//			select {
//			case <-testDone:
//			case <-time.After(100 * time.Millisecond):
//				t.Error(fmt.Sprintf("Test timeout waiting for %d blocks", expectedBlockCount))
//			}
//
//			assert.EqualValues(t, c.expectedBlockIDs, receivedBlockIDs)
//			//			ifs.Shutdown(nil)
//
//		})
//	}
//
//}
