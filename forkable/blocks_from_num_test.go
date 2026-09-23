package forkable

import (
	"testing"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Some chains (Solana, NEAR, ...) can skip block numbers: requesting a start
// block that was skipped must serve from the first block above it instead of
// failing forever.
func TestCallWithBlocksFromNum_SkippedBlockNums(t *testing.T) {
	p := New(
		bstream.HandlerFunc(func(blk *pbbstream.Block, obj any) error { return nil }),
		HoldBlocksUntilLIB(),
		WithKeptFinalBlocks(100),
	)

	blocks := []*pbbstream.Block{
		bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
		bstream.TestBlockWithLIBNum("00000004", "00000003", 3),
		bstream.TestBlockWithLIBNum("00000006", "00000004", 3), // number 5 skipped by the chain
		bstream.TestBlockWithLIBNum("00000007", "00000006", 3),
	}
	for _, blk := range blocks {
		require.NoError(t, p.ProcessBlock(blk, nil))
	}

	blockNumsFrom := func(num uint64) (out []uint64, err error) {
		err = p.CallWithBlocksFromNum(num, func(blks []*bstream.PreprocessedBlock) {
			for _, b := range blks {
				out = append(out, b.Num())
			}
		}, false)
		return
	}

	t.Run("requesting a skipped block num starts at the next available block", func(t *testing.T) {
		nums, err := blockNumsFrom(5)
		require.NoError(t, err)
		assert.Equal(t, []uint64{6, 7}, nums)
	})

	t.Run("requesting an existing block num still starts exactly there", func(t *testing.T) {
		nums, err := blockNumsFrom(4)
		require.NoError(t, err)
		assert.Equal(t, []uint64{4, 6, 7}, nums)

		nums, err = blockNumsFrom(3)
		require.NoError(t, err)
		assert.Equal(t, []uint64{3, 4, 6, 7}, nums)
	})

	t.Run("requesting below the lowest held block fails so caller can use another source", func(t *testing.T) {
		_, err := blockNumsFrom(2)
		require.Error(t, err)
	})

	t.Run("requesting above head fails", func(t *testing.T) {
		_, err := blockNumsFrom(8)
		require.Error(t, err)
	})
}
