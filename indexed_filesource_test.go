package bstream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/test-go/testify/require"
)

func TestFileSource_WrapObjectWithCursor(t *testing.T) {
	blk := &Block{
		Id:         "00000004a",
		PreviousId: "00000003a",
		Number:     4,
		LibNum:     1,
	}

	obj := "hello"

	s := &IndexedFileSource{}
	wobj := s.wrapObjectWithCursor(obj, blk.AsRef(), StepIrreversible)

	assert.NotNil(t, wobj)
	assert.NotNil(t, wobj.Cursor())
	assert.Equal(t, "c1:16:4:00000004a:4:00000004a", wobj.Cursor().String())

	s.cursor = &Cursor{
		Step:      StepNew,
		Block:     &Block{Id: "aa", Number: 202},
		HeadBlock: &Block{Id: "aa", Number: 202},
		LIB:       &Block{Id: "22", Number: 2},
	}
	blk = &Block{
		Id:     "33",
		Number: 3,
	}

	wobj = s.wrapObjectWithCursor(obj, blk.AsRef(), StepIrreversible)

	assert.NotNil(t, wobj)
	assert.NotNil(t, wobj.Cursor())
	assert.Equal(t, "c2:16:3:33:202:aa", wobj.Cursor().String()) // head block still points to 202

}

func TestFileSource_BlockIndexesManager_IrrOnly(t *testing.T) {
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
			name: "skip forked blocks",
			files: map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
				6, "6b", "4a", 0,
				99, "99a", "6a", 0,
			),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			cursor: nil,
			expected: expected{
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
			name: "fill missing irreversible blocks on cursor stepIrreversible",
			files: map[int][]byte{
				0: testBlocks(
					1, "1a", "1a", 0,
					2, "2a", "1a", 0,
					3, "3a", "2a", 0,
					4, "4a", "3a", 0,
					7, "7a", "4a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {1: "1a", 2: "2a", 3: "3a", 4: "4a", 7: "7a"},
				},
			},
			cursor: &Cursor{
				Step:      StepIrreversible,
				LIB:       BasicBlockRef{"1a", 1},
				HeadBlock: BasicBlockRef{"3a", 3},
				Block:     BasicBlockRef{"1a", 1},
			},
			expected: expected{
				[]string{"2a", "3a", "4a", "4a", "7a", "7a"},
				[]StepType{StepIrreversible, StepIrreversible, StepNew, StepIrreversible, StepNew, StepIrreversible},
				BasicBlockRef{"7a", 7},
			},
		},
		{
			name: "fill missing irreversible blocks on cursor stepNew",
			files: map[int][]byte{
				0: testBlocks(
					1, "1a", "1a", 0,
					2, "2a", "1a", 0,
					3, "3a", "2a", 0,
					4, "4a", "3a", 0,
					7, "7a", "4a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {1: "1a", 2: "2a", 3: "3a", 4: "4a", 7: "7a"},
				},
			},
			cursor: &Cursor{
				Step:      StepNew,
				LIB:       BasicBlockRef{"1a", 1},
				HeadBlock: BasicBlockRef{"3a", 3},
				Block:     BasicBlockRef{"3a", 3},
			},
			expected: expected{
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
				blockIndexManager:       NewBlockIndexesManager(context.Background(), irrStore, bundleSizes, startBlockNum, 0, mustMatch, nil),
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

func TestFileSource_BlockIndexesManager_WithExtraIndexProvider(t *testing.T) {

	type blockIDStep struct {
		blockID    string
		blockSteps StepType
	}
	type expected struct {
		blocks         []blockIDStep
		nextHandlerLIB BlockRef
	}

	tests := []struct {
		name                      string
		files                     map[int][]byte
		irreversibleBlocksIndexes map[int]map[int]map[int]string
		blockIndexProvider        BlockIndexProvider
		stopBlockNum              uint64
		expected                  expected
	}{
		{
			name: "skip forked and non-matching blocks, nextHandlerLIB non-matching",
			files: map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
				6, "6b", "4a", 0,
				8, "8b", "6a", 0,
				99, "99a", "6a", 0,
			),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4:  "4a",
						6:  "6a",
						99: "99a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{ // matches 4 and 6 only
				withinRange: map[uint64]bool{4: true},
				matches: map[uint64]matchesResp{
					1: {false, nil},
					4: {true, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:  {4, false, nil},
					4:  {6, false, nil},
					6:  {100, true, nil},
					99: {100, true, nil},
				},
			},
			expected: expected{
				blocks: []blockIDStep{
					{"4a", StepNew},
					{"4a", StepIrreversible},
					{"6a", StepNew},
					{"6a", StepIrreversible},
				},
				nextHandlerLIB: BasicBlockRef{"99a", 99}, // ready for forkable to start at block 100 with LIB=99a
			},
		},
		{
			name: "empty index provider transition to irreversible index only",
			files: map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
				8, "8a", "6a", 0,
			),
				100: testBlocks(
					100, "100a", "8a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
						8: "8a",
					},
					100: {
						100: "100a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{
				withinRange: map[uint64]bool{
					4:   true,
					100: true,
				},
				matches: map[uint64]matchesResp{
					1:   {false, nil},
					4:   {false, nil},
					100: {false, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:   {100, true, nil},
					4:   {100, true, nil},
					99:  {100, true, nil},
					100: {100, true, nil},
				},
			},
			expected: expected{
				blocks: []blockIDStep{
					{"4a", StepNew},
					{"4a", StepIrreversible},
					{"100a", StepNew},
					{"100a", StepIrreversible},
				},
				nextHandlerLIB: BasicBlockRef{"100a", 100},
			},
		},
		{
			name: "empty index provider transition to unindexed",
			files: map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
			),
				100: testBlocks(
					100, "100a", "6a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{
				withinRange: map[uint64]bool{
					4:   true,
					100: true,
				},
				matches: map[uint64]matchesResp{
					1:   {false, nil},
					4:   {false, nil},
					100: {false, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:  {100, true, nil},
					4:  {100, true, nil},
					99: {100, true, nil},
				},
			},
			expected: expected{
				blocks: []blockIDStep{
					{"4a", StepNew},
					{"4a", StepIrreversible},
					{"100a", StepNew},
				},
				nextHandlerLIB: BasicBlockRef{"6a", 6},
			},
		},
		{
			name: "index match on start block",
			files: map[int][]byte{
				0: testBlocks(
					1, "1a", "0a", 0,
					4, "4a", "3a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						1: "1a",
						4: "4a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{ // [0-100]: {4,6}
				withinRange: map[uint64]bool{
					1: true,
					4: true,
				},
				matches: map[uint64]matchesResp{
					1: {true, nil},
					4: {true, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:  {4, false, nil},
					4:  {100, true, nil},
					99: {100, true, nil},
				},
			},
			expected: expected{
				blocks: []blockIDStep{
					{"1a", StepNew},
					{"1a", StepIrreversible},
					{"4a", StepNew},
					{"4a", StepIrreversible},
				},
				nextHandlerLIB: BasicBlockRef{"4a", 4},
			},
		},

		{
			name: "duplicates",
			files: map[int][]byte{
				0: testBlocks(
					1, "1a", "0a", 0,
					1, "1a", "0a", 0,
					1, "1a", "0a", 0,
					3, "3a", "2a", 0,
					3, "3a", "2a", 0,
					1, "1a", "0a", 0,
					4, "4a", "3a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						1: "1a",
						3: "3a",
						4: "4a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{ // [0-100]: {4,6}
				withinRange: map[uint64]bool{
					1: true,
					3: true,
					4: true,
				},
				matches: map[uint64]matchesResp{
					1: {true, nil},
					3: {true, nil},
					4: {true, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:  {3, false, nil},
					3:  {4, false, nil},
					4:  {100, true, nil},
					99: {100, true, nil},
				},
			},
			expected: expected{
				blocks: []blockIDStep{
					{"1a", StepNew},
					{"1a", StepIrreversible},
					{"3a", StepNew},
					{"3a", StepIrreversible},
					{"4a", StepNew},
					{"4a", StepIrreversible},
				},
				nextHandlerLIB: BasicBlockRef{"4a", 4},
			},
		},

		{
			name: "no match before stop block num will send stop block num",
			files: map[int][]byte{
				0: testBlocks(
					4, "4a", "3a", 0,
					6, "6a", "4a", 0,
					10, "10a", "6a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4:  "4a",
						6:  "6a",
						10: "10a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{ // [0-100]: {4,6}
				withinRange: map[uint64]bool{
					4: true,
					6: true,
				},
				matches: map[uint64]matchesResp{
					1: {false, nil},
					4: {false, nil},
					6: {true, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:  {6, false, nil}, // NextMatching() called with exclusiveUpTo==6, so the provider is expected to return 6 since 10 is above
					4:  {10, false, nil},
					6:  {10, false, nil},
					10: {100, true, nil},
					99: {100, true, nil},
				},
			},
			stopBlockNum: 6,
			expected: expected{
				blocks: []blockIDStep{
					{"4a", StepNew},
					{"4a", StepIrreversible},
					{"6a", StepNew},
					{"6a", StepIrreversible},
				},
				nextHandlerLIB: BasicBlockRef{"10a", 10}, //ready for next handler, but won't get there
			},
		},
		{
			name: "non-empty index provider transition to irr only",
			files: map[int][]byte{
				0: testBlocks(
					4, "4a", "3a", 0,
					6, "6a", "4a", 0,
				),
				100: testBlocks(
					100, "100a", "6a", 0,
				),
			},
			irreversibleBlocksIndexes: map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
					100: {
						100: "100a",
					},
				},
			},
			blockIndexProvider: &mockBlockIndexProvider{ // [0-100]: {4,6}
				withinRange: map[uint64]bool{
					4:   true,
					6:   true,
					100: true,
				},
				matches: map[uint64]matchesResp{
					1:   {false, nil},
					4:   {true, nil},
					6:   {true, nil},
					100: {false, nil},
				},
				nextMatching: map[uint64]nextMatchingResp{
					1:  {4, false, nil},
					4:  {6, false, nil},
					6:  {100, true, nil},
					99: {100, true, nil},
				},
			},
			expected: expected{
				blocks: []blockIDStep{
					{"4a", StepNew},
					{"4a", StepIrreversible},
					{"6a", StepNew},
					{"6a", StepIrreversible},
					{"100a", StepNew},
					{"100a", StepIrreversible},
				},
				nextHandlerLIB: BasicBlockRef{"100a", 100},
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

			expectedBlockCount := len(c.expected.blocks)
			var receivedBlocks []blockIDStep

			expectedBlocksDone := make(chan interface{})
			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
				step := StepNew
				if stepable, ok := obj.(Stepable); ok {
					step = stepable.Step()
				}

				receivedBlocks = append(receivedBlocks, blockIDStep{blk.Id, step})
				if len(receivedBlocks) == expectedBlockCount {
					close(expectedBlocksDone)
				}
				return nil
			})

			startBlockNum := uint64(1)

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

			if c.blockIndexProvider != nil {
				c.blockIndexProvider.(*mockBlockIndexProvider).t = t
			}
			bim := NewBlockIndexesManager(context.Background(), irrStore, bundleSizes, startBlockNum, c.stopBlockNum, nil, c.blockIndexProvider)
			require.NotNil(t, bim)

			ifs := &IndexedFileSource{
				Shutter:                 shutter.New(),
				logger:                  zlog,
				handler:                 handler,
				blockIndexManager:       bim,
				blockStores:             []dstore.Store{bs},
				unindexedSourceFactory:  nextSourceFactory,
				unindexedHandlerFactory: nextHandlerWrapper,
				preprocFunc:             preprocFunc,
				sendNew:                 true,
				sendIrr:                 true,
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

			assert.EqualValues(t, c.expected.blocks, receivedBlocks)
			assert.Equal(t, c.expected.nextHandlerLIB, receivedNextHandlerLIB)

		})
	}

}

type nextMatchingResp struct {
	u uint64
	b bool
	e error
}

type matchesResp struct {
	b bool
	e error
}

type mockBlockIndexProvider struct {
	t            *testing.T
	withinRange  map[uint64]bool
	matches      map[uint64]matchesResp
	nextMatching map[uint64]nextMatchingResp
}

func (p *mockBlockIndexProvider) WithinRange(_ context.Context, blockNum uint64) bool {
	out, ok := p.withinRange[blockNum]
	if !ok {
		p.t.Fatalf("withinRange value not set for %d", blockNum)
	}
	return out
}

func (p *mockBlockIndexProvider) Matches(_ context.Context, blockNum uint64) (bool, error) {
	out, ok := p.matches[blockNum]
	if !ok {
		p.t.Fatalf("matches value not set for %d", blockNum)
	}
	return out.b, out.e
}
func (p *mockBlockIndexProvider) NextMatching(_ context.Context, blockNum uint64, _ uint64) (num uint64, outsideIndexBoundary bool, err error) {
	out, ok := p.nextMatching[blockNum]
	if !ok {
		p.t.Fatalf("nextMatching value not set for %d", blockNum)
	}
	return out.u, out.b, out.e
}
