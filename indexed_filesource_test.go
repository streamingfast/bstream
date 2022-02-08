package bstream

import (
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
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

	wobj := wrapObjectWithCursor(obj, blk.AsRef(), StepIrreversible)

	assert.NotNil(t, wobj)
	assert.NotNil(t, wobj.Cursor())
	assert.Equal(t, "c1:16:4:00000004a:4:00000004a", wobj.Cursor().String())
}

func TestFileSource_IrrIndex(t *testing.T) {
	type expected struct {
		blockIDs       []string
		blockSteps     []StepType
		nextHandlerLIB BlockRef
	}

	tests := []struct {
		name                      string
		files                     map[int][]byte
		irreversibleBlocksIndexes map[int]map[int]map[int]string
		cursor                    *Cursor
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
				[]string{"4a", "4a", "6a", "6a"},
				[]StepType{StepNew, StepIrreversible, StepNew, StepIrreversible},
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
				[]string{"100a", "100a"},
				[]StepType{StepNew, StepIrreversible},
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
				[]string{"4a", "4a", "6a", "6a", "100a"},
				[]StepType{StepNew, StepIrreversible, StepNew, StepIrreversible, StepNew},
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
				[]string{"1a", "1a", "50a", "50a", "75a", "75a", "100a", "100a"},
				[]StepType{StepNew, StepIrreversible, StepNew, StepIrreversible, StepNew, StepIrreversible, StepNew, StepIrreversible},
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
				[]string{"1a", "1a", "50a", "50a", "200a", "200a"},
				[]StepType{StepNew, StepIrreversible, StepNew, StepIrreversible, StepNew, StepIrreversible},
				BasicBlockRef{"200a", 200},
			},
		},
		{
			"cursor skips step new",
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
			&Cursor{
				Step:      StepNew,
				LIB:       BasicBlockRef{"2a", 2},
				HeadBlock: BasicBlockRef{"2a", 2},
				Block:     BasicBlockRef{"2a", 2},
			},
			expected{
				[]string{"2a", "3a", "3a", "100a", "100a"},
				[]StepType{StepIrreversible, StepNew, StepIrreversible, StepNew, StepIrreversible},
				BasicBlockRef{"100a", 100},
			},
		},
		{
			"cursor skips step irr",
			map[int][]byte{
				0: testBlocks(
					2, "2b", "1a", 0,
					2, "2a", "1a", 0,
					3, "3a", "2a", 0,
					4, "4a", "3a", 0,
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
			&Cursor{
				Step:      StepIrreversible,
				LIB:       BasicBlockRef{"1a", 2},
				HeadBlock: BasicBlockRef{"2a", 2},
				Block:     BasicBlockRef{"2a", 2},
			},
			expected{
				[]string{"3a", "3a", "100a", "100a"},
				[]StepType{StepNew, StepIrreversible, StepNew, StepIrreversible},
				BasicBlockRef{"100a", 100},
			},
		},
		{
			"perfect cursor handling",
			map[int][]byte{
				0: testBlocks(
					1, "1a", "1a", 0,
					2, "2a", "1a", 0,
					3, "3a", "2a", 0,
					4, "4a", "3a", 0,
					7, "7a", "4a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0: {1: "1a", 2: "2a", 3: "3a", 4: "4a", 7: "7a"},
				},
			},
			&Cursor{
				Step:      StepIrreversible,
				LIB:       BasicBlockRef{"1a", 1},
				HeadBlock: BasicBlockRef{"4a", 4},
				Block:     BasicBlockRef{"3a", 3},
			},
			expected{
				[]string{"2a", "3a", "4a", "4a", "7a", "7a"},
				[]StepType{StepIrreversible, StepIrreversible, StepNew, StepIrreversible, StepNew, StepIrreversible},
				BasicBlockRef{"7a", 7},
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
			var receivedSteps []StepType

			expectedBlocksDone := make(chan interface{})
			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
				receivedBlockIDs = append(receivedBlockIDs, blk.Id)
				step := StepNew
				if stepable, ok := obj.(Stepable); ok {
					step = stepable.Step()
				}
				receivedSteps = append(receivedSteps, step)
				if len(receivedBlockIDs) == expectedBlockCount {
					close(expectedBlocksDone)
				}
				return nil
			})

			startBlockNum := uint64(1)
			if c.cursor != nil {
				startBlockNum = c.cursor.LIB.Num()
			}
			var mustMatch BlockRef
			if c.cursor != nil {
				mustMatch = c.cursor.Block
			}

			preprocFunc := func(blk *Block) (interface{}, error) {
				return nil, nil
			}

			nextSourceFactory := func(startBlock uint64, h Handler) Source {
				fs := NewFileSource(bs, startBlock, 1, preprocFunc, h)
				return fs
			}

			nextHandlerWrapperCalled := make(chan interface{})
			var receivedNextHandlerLIB BlockRef
			nextHandlerWrapper := func(in Handler, lib BlockRef) Handler {
				receivedNextHandlerLIB = lib
				close(nextHandlerWrapperCalled)
				return in
			}

			ifs := &IndexedFileSource{
				Shutter:                 shutter.New(),
				logger:                  zlog,
				handler:                 handler,
				blockIndex:              NewIrreversibleBlocksIndex(irrStore, bundleSizes, startBlockNum, mustMatch),
				blockStores:             []dstore.Store{bs},
				unindexedSourceFactory:  nextSourceFactory,
				unindexedHandlerFactory: nextHandlerWrapper,
				preprocFunc:             preprocFunc,
				sendNew:                 true,
				sendIrr:                 true,
				cursor:                  c.cursor,
			}
			go ifs.Run()

			select {
			case <-expectedBlocksDone:
			case <-time.After(100 * time.Millisecond):
				t.Error(fmt.Sprintf("Test timeout waiting for %d blocks", expectedBlockCount))
			}
			select {
			case <-nextHandlerWrapperCalled:
			case <-time.After(100 * time.Millisecond):
				t.Error("Test timeout waiting for nextHandlerWrapper to be called")
			}

			assert.EqualValues(t, c.expected.blockIDs, receivedBlockIDs)
			assert.EqualValues(t, c.expected.blockSteps, receivedSteps)
			assert.Equal(t, c.expected.nextHandlerLIB, receivedNextHandlerLIB)

		})
	}

}
