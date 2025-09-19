package hub

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/dstore"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var nullSignalHandler = func(*pbbstream.Signal) error {
	return nil
}

func TestForkableHub_Bootstrap(t *testing.T) {
	tests := []struct {
		name                   string
		oneBlocksAvailable     []*pbbstream.Block
		bufferSize             int
		expectedError          error
		expectedForkableLibNum *pbbstream.Block
	}{
		{
			name: "sunny path",
			oneBlocksAvailable: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
			},

			bufferSize:             0,
			expectedError:          nil,
			expectedForkableLibNum: bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
		},

		{
			name: "one block store with random one block files",
			oneBlocksAvailable: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("00000120", "00000119", 288),
				bstream.TestBlockWithLIBNum("00000121", "00000120", 288),
				bstream.TestBlockWithLIBNum("00000122", "00000121", 288),
				bstream.TestBlockWithLIBNum("00000123", "00000122", 290),
				bstream.TestBlockWithLIBNum("00000124", "00000123", 290),
			},

			bufferSize:             0,
			expectedError:          nil,
			expectedForkableLibNum: bstream.TestBlockWithLIBNum("00000122", "00000121", 288),
		},

		{
			name:               "no one block files",
			oneBlocksAvailable: []*pbbstream.Block{},

			bufferSize:             0,
			expectedError:          fmt.Errorf("no one blocks found"),
			expectedForkableLibNum: nil,
		},
		{
			name: "most recent block not linkable",
			oneBlocksAvailable: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("00000120", "00000119", 288),
				bstream.TestBlockWithLIBNum("00000121", "00000120", 288),
				bstream.TestBlockWithLIBNum("00000122", "00000121", 288),
				bstream.TestBlockWithLIBNum("00000124", "00000123", 290),
			},

			bufferSize:             0,
			expectedError:          fmt.Errorf("most recent one block is not linkable"),
			expectedForkableLibNum: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Helper()
			lsf := bstream.NewTestSourceFactory()
			testOneBlockStore := dstore.NewMockStore(nil)

			fh := NewForkableHub(lsf.NewSource, test.bufferSize, testOneBlockStore)

			AddToMockStore(t, testOneBlockStore, test.oneBlocksAvailable...)

			err := fh.bootstrap()
			assert.Equal(t, test.expectedError, err)

			if test.expectedError == nil {
				assert.Equal(t, fh.forkable.LowestBlockNum(), test.expectedForkableLibNum.Number)
			}
		})
	}
}

func TestForkableHub_ProcessBlock(t *testing.T) {
	t.Helper()

	tests := []struct {
		name                     string
		oneBlocksBeforeBootstrap []*pbbstream.Block
		oneBlocksAfterBootstrap  []*pbbstream.Block
		blockToProcess           *pbbstream.Block
		bufferSize               int
		expectedErrorIs          error
		expectedForkableLibNum   uint64
		expectedHeadBlock        string
	}{
		{
			name: "sunny path",
			oneBlocksBeforeBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
			},
			oneBlocksAfterBootstrap: []*pbbstream.Block{},
			blockToProcess:          bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
			bufferSize:              0,
			expectedErrorIs:         nil,
			expectedForkableLibNum:  3,
			expectedHeadBlock:       "00000009",
		},
		{
			name: "reader stopped and restarted",
			oneBlocksBeforeBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
			},

			oneBlocksAfterBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000010", "00000009", 3),
				bstream.TestBlockWithLIBNum("00000011a", "00000010", 3),
				bstream.TestBlockWithLIBNum("00000011b", "00000010", 3),
			},

			blockToProcess:         bstream.TestBlockWithLIBNum("00000012", "00000011a", 3),
			bufferSize:             0,
			expectedErrorIs:        nil,
			expectedForkableLibNum: 3,
			expectedHeadBlock:      "00000012",
		},
		{
			name: "reader stopped and restarted with missing reversible block",
			oneBlocksBeforeBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
			},

			oneBlocksAfterBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000010", "00000009", 3),
				bstream.TestBlockWithLIBNum("00000011b", "00000010", 3),
			},

			blockToProcess:         bstream.TestBlockWithLIBNum("00000012", "00000011a", 3),
			bufferSize:             0,
			expectedErrorIs:        nil,
			expectedForkableLibNum: 3,
			expectedHeadBlock:      "00000011b",
		},

		{
			name: "reader stopped and restarted with missing reversible block",
			oneBlocksBeforeBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
			},

			oneBlocksAfterBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000010", "00000009", 8),
				bstream.TestBlockWithLIBNum("00000011b", "00000010", 8),
			},

			blockToProcess:         bstream.TestBlockWithLIBNum("00000012", "00000011a", 0x10),
			bufferSize:             0,
			expectedErrorIs:        errRestartRequired,
			expectedForkableLibNum: 3,
			expectedHeadBlock:      "00000011b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsf := bstream.NewTestSourceFactory()
			testOneBlockStore := dstore.NewMockStore(nil)

			fh := NewForkableHub(lsf.NewSource, test.bufferSize, testOneBlockStore)

			AddToMockStore(t, testOneBlockStore, test.oneBlocksBeforeBootstrap...)

			err := fh.bootstrap()
			if err != nil {
				require.NoError(t, err)
			}

			AddToMockStore(t, testOneBlockStore, test.oneBlocksAfterBootstrap...)

			err = fh.ProcessBlock(test.blockToProcess, nil)

			if test.expectedErrorIs != nil {
				assert.True(t, errors.Is(err, test.expectedErrorIs))
			} else {
				assert.Equal(t, fh.forkable.LowestBlockNum(), test.expectedForkableLibNum)

				_, headID, _, _, err := fh.forkable.HeadInfo()
				require.NoError(t, err)
				assert.Equal(t, test.expectedHeadBlock, headID)
			}
		})
	}
}

func TestForkableHub_Run(t *testing.T) {
	t.Helper()

	tests := []struct {
		name                     string
		oneBlocksBeforeBootstrap []*pbbstream.Block
		oneBlocksAfterBootstrap  []*pbbstream.Block
		blockToProcess           *pbbstream.Block
		bufferSize               int
		expectedForkableLibNum   uint64
		expectedHeadBlock        string
	}{
		{
			name: "sunny path",
			oneBlocksBeforeBootstrap: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
			},
			oneBlocksAfterBootstrap: []*pbbstream.Block{},
			blockToProcess:          bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
			bufferSize:              0,
			expectedForkableLibNum:  3,
			expectedHeadBlock:       "00000009",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsf := bstream.NewTestSourceFactory()
			testOneBlockStore := dstore.NewMockStore(nil)

			testSource := lsf.NewSource

			fh := NewForkableHub(testSource, test.bufferSize, testOneBlockStore)

			AddToMockStore(t, testOneBlockStore, test.oneBlocksBeforeBootstrap...)

			go fh.Run()

			select {
			case <-fh.Ready:
			case <-time.After(1 * time.Second):
				t.Fail()
			}

			AddToMockStore(t, testOneBlockStore, test.oneBlocksAfterBootstrap...)
			ls := <-lsf.Created

			err := ls.Push(test.blockToProcess, nil)
			if err != nil {
				require.NoError(t, err)
			}

			assert.Equal(t, fh.forkable.LowestBlockNum(), test.expectedForkableLibNum)

			_, headID, _, _, err := fh.forkable.HeadInfo()
			require.NoError(t, err)
			assert.Equal(t, test.expectedHeadBlock, headID)
		})
	}
}

type expectedBlock struct {
	block        *pbbstream.Block
	step         bstream.StepType
	cursorLibNum uint64
}

func AddToMockStore(t *testing.T, store *dstore.MockStore, blocks ...*pbbstream.Block) {
	t.Helper()

	for _, block := range blocks {
		buffer := bytes.NewBuffer(nil)
		blockWriter, err := bstream.NewDBinBlockWriter(buffer)
		require.NoError(t, err)

		pbbstreamBlock := block

		err = blockWriter.Write(pbbstreamBlock)
		require.NoError(t, err)

		store.SetFile(bstream.BlockFileName(pbbstreamBlock), buffer.Bytes())
	}
}

func TestForkableHub_SourceFromCursor(t *testing.T) {

	tests := []struct {
		name          string
		forkdbBlocks  []*pbbstream.Block
		requestCursor *bstream.Cursor
		expectBlocks  []expectedBlock
	}{
		{
			name: "irr and new",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("0000000a", "00000009", 4),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRefFromID("00000005"),
				HeadBlock: bstream.NewBlockRefFromID("00000008"),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
					bstream.StepIrreversible,
					4,
				},
				{
					bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
					bstream.StepNew,
					4,
				},
				{
					bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
					bstream.StepNew,
					4,
				},
				{
					bstream.TestBlockWithLIBNum("0000000a", "00000009", 4),
					bstream.StepNew,
					4,
				},
			},
		},
		{
			name: "cursor head in future",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRefFromID("00000005"),
				HeadBlock: bstream.NewBlockRefFromID("00000005"),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
		},
		{
			name: "cursor block on head",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRefFromID("00000004"),
				HeadBlock: bstream.NewBlockRefFromID("00000004"),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
			expectBlocks: []expectedBlock{},
		},
		{
			name: "cursor block on head, lib moved",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 4),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRefFromID("00000005"),
				HeadBlock: bstream.NewBlockRefFromID("00000005"),
				LIB:       bstream.NewBlockRefFromID("00000003"), // hub has LIB at 4
			},
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
					bstream.StepIrreversible,
					4,
				},
			},
		},
		{
			name: "cursor on forked block",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
				bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000005b","prev":"00000004","number":5,"libnum":3}`)),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRef("00000005b", 5),
				HeadBlock: bstream.NewBlockRef("00000005b", 5),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000005b","prev":"00000004","number":5,"libnum":3}`)),
					bstream.StepUndo,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "cursor on undo step",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
				bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000005b","prev":"00000004","number":5,"libnum":3}`)),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepUndo,
				Block:     bstream.NewBlockRef("00000005b", 5),
				HeadBlock: bstream.NewBlockRef("00000005b", 5),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "cursor on undo step same block",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
				bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000005b","prev":"00000004","number":5,"libnum":3}`)),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepUndo,
				Block:     bstream.NewBlockRef("00000005", 5),
				HeadBlock: bstream.NewBlockRef("00000005", 5),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "cursor on deep fork",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
				bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000005b","prev":"00000004","number":5,"libnum":3}`)),
				bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000006b","prev":"00000005b","number":6,"libnum":3}`)),
				bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000007b","prev":"00000006b","number":7,"libnum":3}`)),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000007", "00000006", 3),
				bstream.TestBlockWithLIBNum("00000008", "00000007", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepUndo,
				Block:     bstream.NewBlockRef("00000007b", 5),
				HeadBlock: bstream.NewBlockRef("00000007b", 5),
				LIB:       bstream.NewBlockRefFromID("00000003"),
			},
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000006b","prev":"00000005b","number":6,"libnum":3}`)),
					bstream.StepUndo,
					3,
				},
				{
					bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"00000005b","prev":"00000004","number":5,"libnum":3}`)),
					bstream.StepUndo,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000006", "00000005", 3),
					bstream.StepNew,
					3,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fh := &ForkableHub{
				Shutter: shutter.New(),
			}
			fh.forkable = forkable.New(bstream.NewHandler(fh.broadcastBlock, nullSignalHandler),
				forkable.HoldBlocksUntilLIB(),
				forkable.WithKeptFinalBlocks(100),
			)

			for _, blk := range test.forkdbBlocks {
				require.NoError(t, fh.forkable.ProcessBlock(blk, nil))
			}

			var seenBlocks []expectedBlock
			handler := bstream.NewHandler(func(blk *pbbstream.Block, obj any) error {
				seenBlocks = append(seenBlocks, expectedBlock{blk, obj.(*forkable.ForkableObject).Step(), obj.(*forkable.ForkableObject).Cursor().LIB.Num()})
				if len(seenBlocks) == len(test.expectBlocks) {
					return fmt.Errorf("done")
				}
				return nil
			}, nullSignalHandler)
			source := fh.SourceFromCursor(test.requestCursor, handler)
			if test.expectBlocks == nil {
				assert.Nil(t, source)
				return
			}

			require.NotNil(t, source)

			if len(test.expectBlocks) == 0 {
				return // we get an empty source for now, until live blocks come in
			}
			go source.Run()
			select {
			case <-source.Terminating():
				assertExpectedBlocks(t, test.expectBlocks, seenBlocks)
			case <-time.After(time.Second):
				t.Errorf("timeout waiting for blocks")
			}
		})
	}
}

func TestForkableHub_SourceThroughCursor(t *testing.T) {

	tests := []struct {
		name              string
		forkdbBlocks      []*pbbstream.Block
		requestCursor     *bstream.Cursor
		requestStartBlock uint64
		expectBlocks      []expectedBlock
	}{
		{
			name: "through canonical cursor",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
				bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
				bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2), //fork
				bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
				bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRef("00000004a", 4),
				HeadBlock: bstream.NewBlockRef("00000005a", 5),
				LIB:       bstream.NewBlockRef("00000003a", 3),
			},
			requestStartBlock: 3,
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
					bstream.StepNewIrreversible,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "through canonical undo cursor",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
				bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
				bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2), //fork
				bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
				bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepUndo,
				Block:     bstream.NewBlockRef("00000004a", 4),
				HeadBlock: bstream.NewBlockRef("00000005a", 5),
				LIB:       bstream.NewBlockRef("00000003a", 3),
			},
			requestStartBlock: 3,
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
					bstream.StepNewIrreversible,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "through non-canonical cursor",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
				bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
				bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2), //fork
				bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
				bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRef("00000004b", 4),
				HeadBlock: bstream.NewBlockRef("00000004b", 4),
				LIB:       bstream.NewBlockRef("00000003a", 3),
			},
			requestStartBlock: 3,
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
					bstream.StepNewIrreversible,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2),
					bstream.StepUndo,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "through deep non-canonical cursor",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
				bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
				bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2), //fork
				bstream.TestBlockWithLIBNum("00000005b", "00000004b", 2), //fork
				bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
				bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRef("00000005b", 5),
				HeadBlock: bstream.NewBlockRef("00000005b", 5),
				LIB:       bstream.NewBlockRef("00000003a", 3),
			},
			requestStartBlock: 3,
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
					bstream.StepNewIrreversible,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005b", "00000004b", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005b", "00000004b", 2),
					bstream.StepUndo,
					3,
				},

				{
					bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2),
					bstream.StepUndo,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "through deep non-canonical UNDO cursor",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
				bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
				bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2), //fork
				bstream.TestBlockWithLIBNum("00000005b", "00000004b", 2), //fork
				bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
				bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepUndo,
				Block:     bstream.NewBlockRef("00000005b", 5),
				HeadBlock: bstream.NewBlockRef("00000005b", 5),
				LIB:       bstream.NewBlockRef("00000003a", 3),
			},
			requestStartBlock: 3,
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
					bstream.StepNewIrreversible,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2),
					bstream.StepNew,
					3,
				},

				{
					bstream.TestBlockWithLIBNum("00000004b", "00000003a", 2),
					bstream.StepUndo,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
					bstream.StepNew,
					3,
				},
				{
					bstream.TestBlockWithLIBNum("00000008a", "00000005a", 3),
					bstream.StepNew,
					3,
				},
			},
		},
		{
			name: "start block too low no source",
			forkdbBlocks: []*pbbstream.Block{
				bstream.TestBlockWithLIBNum("00000003a", "00000002a", 2),
				bstream.TestBlockWithLIBNum("00000004a", "00000003a", 2),
				bstream.TestBlockWithLIBNum("00000005a", "00000004a", 3),
			},
			requestCursor: &bstream.Cursor{
				Step:      bstream.StepNew,
				Block:     bstream.NewBlockRef("00000005a", 5),
				HeadBlock: bstream.NewBlockRef("00000005a", 5),
				LIB:       bstream.NewBlockRef("00000003a", 3),
			},
			requestStartBlock: 2,
			expectBlocks:      nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fh := &ForkableHub{
				Shutter: shutter.New(),
			}
			fh.forkable = forkable.New(bstream.NewHandler(fh.broadcastBlock, nullSignalHandler),
				forkable.HoldBlocksUntilLIB(),
				forkable.WithKeptFinalBlocks(100),
			)

			for _, blk := range test.forkdbBlocks {
				require.NoError(t, fh.forkable.ProcessBlock(blk, nil))
			}

			var seenBlocks []expectedBlock
			handler := bstream.NewHandler(func(blk *pbbstream.Block, obj any) error {
				seenBlocks = append(seenBlocks, expectedBlock{blk, obj.(*forkable.ForkableObject).Step(), obj.(*forkable.ForkableObject).Cursor().LIB.Num()})
				if len(seenBlocks) == len(test.expectBlocks) {
					return fmt.Errorf("done")
				}
				return nil
			}, nullSignalHandler)

			source := fh.SourceThroughCursor(test.requestStartBlock, test.requestCursor, handler)
			if test.expectBlocks == nil {
				assert.Nil(t, source)
				return
			}

			require.NotNil(t, source)

			if len(test.expectBlocks) == 0 {
				return // we get an empty source for now, until live blocks come in
			}
			go source.Run()
			select {
			case <-source.Terminating():
				assertExpectedBlocks(t, test.expectBlocks, seenBlocks)
			case <-time.After(time.Second):
				t.Errorf("timeout waiting for blocks")
			}
		})
	}
}
