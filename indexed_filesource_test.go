package bstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
)

func TestFileSource_WrapIrrObjWithCursor(t *testing.T) {
	blk := &Block{
		Id:         "00000004a",
		PreviousId: "00000003a",
		Number:     4,
		LibNum:     1,
	}

	obj := "hello"

	wobj := wrapIrreversibleBlockWithCursor(blk, obj, StepIrreversible)

	assert.NotNil(t, wobj)
	assert.NotNil(t, wobj.Cursor())
	assert.Equal(t, "c1:16:4:00000004a:4:00000004a", wobj.Cursor().String())
}

func TestFileSource_IrrIndex(t *testing.T) {
	type expected struct {
		blockIDs       []string
		nextHandlerLIB BlockRef
	}

	tests := []struct {
		name                      string
		files                     map[int][]byte
		irreversibleBlocksIndexes map[int]map[int]map[int]string
		cursorBlockRef            BlockRef
		expected                  expected
	}{
		{
			"skip forked blocks",
			map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
				6, "6b", "4a", 0,
				99, "99a", "6a", 0,
			),
			},
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			nil,
			expected{
				[]string{"4a", "6a"},
				BasicBlockRef{"6a", 6},
			},
		},
		{
			"skip everything if empty index",
			map[int][]byte{
				0: testBlocks(
					4, "4a", "3a", 0,
					6, "6a", "4a", 0,
				),
				100: testBlocks(
					100, "100a", "zz", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {},
					100: {100: "100a"},
				},
			},
			nil,
			expected{
				[]string{"100a"},
				BasicBlockRef{"100a", 100},
			},
		},
		{
			"transition to unindexed range",
			map[int][]byte{
				0: testBlocks(
					4, "4a", "zz", 0,
					6, "6a", "4a", 0,
				),
				100: testBlocks(
					100, "100a", "6a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0: {4: "4a", 6: "6a"},
				},
			},
			nil,
			expected{
				[]string{"4a", "6a", "100a"},
				BasicBlockRef{"6a", 6},
			},
		},
		{
			"irreversible blocks in next block file",
			map[int][]byte{
				0: testBlocks(
					1, "1a", "0a", 0,
					50, "50a", "1a", 0,
					75, "75b", "50a", 0,
				),
				100: testBlocks(
					75, "75a", "50a", 0, // this DOES happen with forky chains
					100, "100a", "75a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {1: "1a", 50: "50a", 75: "75a"},
					100: {100: "100a"},
				},
			},
			nil,
			expected{
				[]string{"1a", "50a", "75a", "100a"},
				BasicBlockRef{"100a", 100},
			},
		},
		{
			"large file takes precedence over small files",
			map[int][]byte{
				0: testBlocks(
					1, "1a", "0a", 0,
					50, "50a", "1a", 0,
				),
				100: testBlocks(
					100, "100a", "50a", 0,
				),
				200: testBlocks(
					200, "200a", "50a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {1: "1a", 50: "50a"},
					100: {100: "NONEXISTING"}, // this is not a real life scenario, only for testing
				},
				1000: {
					0: {1: "1a", 50: "50a", 200: "200a"},
				},
			},
			nil,
			expected{
				[]string{"1a", "50a", "200a"},
				BasicBlockRef{"200a", 200},
			},
		},
		{
			"use index if cursor is on a valid block",
			map[int][]byte{
				0: testBlocks(
					2, "2a", "1a", 0,
					2, "2b", "1a", 0,
					3, "3a", "2a", 0,
				),
				100: testBlocks(
					100, "100a", "3a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {2: "2a", 3: "3a"},
					100: {100: "100a"},
				},
			},
			BasicBlockRef{"2a", 2},
			expected{
				[]string{"2a", "3a", "100a"},
				BasicBlockRef{"100a", 100},
			},
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			bs := dstore.NewMockStore(nil)
			for i, data := range c.files {
				bs.SetFile(base(i), data)
			}

			irrStore, bundleSizes := getIrrStore(c.irreversibleBlocksIndexes)

			expectedBlockCount := len(c.expected.blockIDs)
			var receivedBlockIDs []string

			testDone := make(chan interface{})
			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
				if steppable, ok := obj.(Stepable); ok {
					if steppable.Step() == StepNew { // skip the 'new' blocks from index, which are also sent as irreversible right after
						return nil
					}
				}
				receivedBlockIDs = append(receivedBlockIDs, blk.Id)
				if len(receivedBlockIDs) == expectedBlockCount {
					close(testDone)
				}
				return nil
			})

			startBlockNum := uint64(1)
			if c.cursorBlockRef != nil {
				startBlockNum = c.cursorBlockRef.Num()
			}
			var mustMatch BlockRef
			if c.cursorBlockRef != nil {
				mustMatch = c.cursorBlockRef
			}

			preprocFunc := func(blk *Block) (interface{}, error) {
				return nil, nil
			}

			nextSourceFactory := func(startBlock uint64, h Handler) Source {
				fs := NewFileSource(bs, startBlock, 1, preprocFunc, h)
				return fs
			}
			var receivedNextHandlerLIB BlockRef
			nextHandlerWrapper := func(in Handler, lib BlockRef) Handler {
				receivedNextHandlerLIB = lib
				return in
			}

			ifs := &IndexedFileSource{
				logger:             zlog,
				handler:            handler,
				blockIndex:         NewIrreversibleBlocksIndex(irrStore, bundleSizes, startBlockNum, mustMatch),
				blockStore:         bs,
				nextSourceFactory:  nextSourceFactory,
				nextHandlerWrapper: nextHandlerWrapper,
				preprocFunc:        preprocFunc,
			}
			go ifs.Run()

			select {
			case <-testDone:
			case <-time.After(100 * time.Millisecond):
				t.Error(fmt.Sprintf("Test timeout waiting for %d blocks", expectedBlockCount))
			}

			assert.EqualValues(t, c.expected.blockIDs, receivedBlockIDs)
			assert.Equal(t, c.expected.nextHandlerLIB, receivedNextHandlerLIB)

		})
	}

}
