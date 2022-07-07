package hub

import (
	"fmt"
	"io"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForkableHub_Bootstrap(t *testing.T) {
	tests := []struct {
		name                       string
		liveBlocks                 []*bstream.Block
		oneBlocksPasses            [][]*bstream.Block
		bufferSize                 int
		expectStartNum             uint64
		expectReady                bool
		expectReadyAfter           bool
		expectHeadRef              bstream.BlockRef
		expectBlocksInCurrentChain []uint64
	}{
		{
			name: "vanilla",
			liveBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("0000000a", "00000009", 4),
			},
			oneBlocksPasses: [][]*bstream.Block{{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
			}},
			bufferSize:                 0,
			expectStartNum:             0,
			expectReady:                true,
			expectReadyAfter:           true,
			expectHeadRef:              bstream.NewBlockRefFromID("00000009"),
			expectBlocksInCurrentChain: []uint64{4, 5, 8, 9},
		},
		{
			name: "LIB met on second live block",
			liveBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("0000000a", "00000009", 5),
			},
			oneBlocksPasses: [][]*bstream.Block{
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
					bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				},
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
					bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				},
			},
			bufferSize:                 0,
			expectStartNum:             0,
			expectReady:                false,
			expectReadyAfter:           true,
			expectHeadRef:              bstream.NewBlockRefFromID("0000000a"),
			expectBlocksInCurrentChain: []uint64{5, 8, 9, 10},
		},
		{
			name: "cannot join one-block-files",
			liveBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("0000000a", "00000009", 4),
			},
			oneBlocksPasses: [][]*bstream.Block{
				{
					bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
					bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
					bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				},
				{
					bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
					bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
					bstream.TestBlockWithLIBNum("00000005", "00000004", 3),
				},
			},
			bufferSize:       0,
			expectStartNum:   0,
			expectReady:      false,
			expectReadyAfter: false,
		},

		{
			name: "one-block-file joined eventually",
			liveBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000008", "00000007", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
			},
			oneBlocksPasses: [][]*bstream.Block{
				{
					bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
					bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
					bstream.TestBlockWithLIBNum("00000006", "00000004", 3),
				},
				{
					bstream.TestBlockWithLIBNum("00000003", "00000002", 3),
					bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
					bstream.TestBlockWithLIBNum("00000006", "00000004", 3),
					bstream.TestBlockWithLIBNum("00000007", "00000006", 3),
				},
			},
			bufferSize:                 2,
			expectStartNum:             0,
			expectReady:                false,
			expectReadyAfter:           true,
			expectHeadRef:              bstream.NewBlockRefFromID("00000009"),
			expectBlocksInCurrentChain: []uint64{3, 4, 6, 7, 8, 9},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lsf := bstream.NewTestSourceFactory()
			obsf := bstream.NewTestSourceFactory()
			fh := NewForkableHub(lsf.NewSource, obsf.NewSourceFromNum, test.bufferSize)

			go fh.Run()

			ls := <-lsf.Created

			// sending oneblockfiles on demand
			go func() {
				for _, oneBlocks := range test.oneBlocksPasses {
					obs := <-obsf.Created
					assert.Equal(t, test.expectStartNum, obs.StartBlockNum)
					for _, blk := range oneBlocks {
						require.NoError(t, obs.Push(blk, nil))
					}
					obs.Shutdown(io.EOF)
				}
			}()

			if err := ls.Push(test.liveBlocks[0], nil); err != nil {
				ls.Shutdown(err)
			}

			assert.Equal(t, test.expectReady, fh.Ready)

			for _, blk := range test.liveBlocks[1:] {
				require.NoError(t, ls.Push(blk, nil))
			}
			assert.Equal(t, test.expectReadyAfter, fh.Ready)

			if test.expectHeadRef != nil || test.expectBlocksInCurrentChain != nil {
				var seenBlockNums []uint64
				chain, _ := fh.forkdb.CompleteSegment(test.expectHeadRef)
				for _, blk := range chain {
					seenBlockNums = append(seenBlockNums, blk.BlockNum)
				}
				assert.Equal(t, test.expectBlocksInCurrentChain, seenBlockNums)
			}

			assert.False(t, fh.IsTerminating())

		})
	}
}

func TestForkableHub_SourceFromFinalBlock(t *testing.T) {

	type expectedBlock struct {
		block *bstream.Block
		step  bstream.StepType
		//cursor string
	}

	tests := []struct {
		name         string
		forkdbBlocks []*bstream.Block
		requestBlock bstream.BlockRef
		expectBlocks []expectedBlock
	}{
		{
			name: "vanilla",
			forkdbBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("0000000a", "00000009", 4),
			},
			requestBlock: bstream.NewBlockRefFromID("00000005"),
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
					bstream.StepNew,
				},

				{
					bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
					bstream.StepNew,
				},
				{
					bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
					bstream.StepNew,
				},

				{
					bstream.TestBlockWithLIBNum("0000000a", "00000009", 4),
					bstream.StepNew,
				},
			},
		},
		{
			name: "step_irreversible",
			forkdbBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 4),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 5),
				bstream.TestBlockWithLIBNum("0000000a", "00000009", 8),
			},
			requestBlock: bstream.NewBlockRefFromID("00000003"),
			expectBlocks: []expectedBlock{
				{
					bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
					bstream.StepIrreversible,
				},
				{
					bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
					bstream.StepIrreversible,
				},
				{
					bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
					bstream.StepIrreversible,
				},

				{
					bstream.TestBlockWithLIBNum("00000008", "00000005", 4),
					bstream.StepIrreversible,
				},
				{
					bstream.TestBlockWithLIBNum("00000009", "00000008", 5),
					bstream.StepNew,
				},

				{
					bstream.TestBlockWithLIBNum("0000000a", "00000009", 8),
					bstream.StepNew,
				},
			},
		},

		{
			name: "no source",
			forkdbBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
			},
			requestBlock: bstream.NewBlockRefFromID("00000005"),
		},
		{
			name: "no source cause wrong block",
			forkdbBlocks: []*bstream.Block{
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
			},
			requestBlock: bstream.NewBlockRef("00000033", 3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			fh := &ForkableHub{
				Shutter: shutter.New(),
				forkdb:  forkable.NewForkDB(),
			}
			fh.forkable = forkable.New(fh,
				forkable.WithForkDB(fh.forkdb),
				forkable.HoldBlocksUntilLIB(),
				forkable.WithKeptFinalBlocks(100),
			)
			fh.Ready = true

			for _, blk := range test.forkdbBlocks {
				fh.forkable.ProcessBlock(blk, nil)
			}

			var seenBlocks []expectedBlock
			handler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
				seenBlocks = append(seenBlocks, expectedBlock{blk, obj.(*forkable.ForkableObject).Step()})
				if len(seenBlocks) == len(test.expectBlocks) {
					return fmt.Errorf("done")
				}
				return nil
			})
			source := fh.SourceFromFinalBlock(handler, test.requestBlock)
			if test.expectBlocks == nil {
				assert.Nil(t, source)
				return
			}
			go source.Run()
			<-source.Terminating()
			assert.Equal(t, test.expectBlocks, seenBlocks)
		})
	}
}
