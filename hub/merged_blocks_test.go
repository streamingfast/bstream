package hub

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testChain returns blocks from..to, each one final two blocks later.
func testChain(from, to uint64) (out []*pbbstream.Block) {
	id := func(num uint64) string { return fmt.Sprintf("%08x", num) }
	for num := from; num <= to; num++ {
		out = append(out, bstream.TestBlockWithLIBNum(id(num), id(num-1), num-2))
	}
	return out
}

func setMergedBlocksFiles(store *dstore.MockStore, bases ...uint64) {
	for _, base := range bases {
		store.SetFile(mergedBlocksFilename(base), []byte("merged"))
	}
}

func newMergedBlocksTestHub(t *testing.T, oneBlocks []*pbbstream.Block, mergedBases []uint64, maxKeptFinalBlocks int) (*ForkableHub, *dstore.MockStore) {
	t.Helper()

	prev := bstream.GetProtocolFirstStreamableBlock
	bstream.GetProtocolFirstStreamableBlock = 1
	t.Cleanup(func() { bstream.GetProtocolFirstStreamableBlock = prev })

	oneBlocksStore := dstore.NewMockStore(nil)
	AddToMockStore(t, oneBlocksStore, oneBlocks...)

	mergedBlocksStore := dstore.NewMockStore(nil)
	setMergedBlocksFiles(mergedBlocksStore, mergedBases...)

	lsf := bstream.NewTestSourceFactory()
	fh := NewForkableHubWithOptions(lsf.NewSource, 0, oneBlocksStore, []Option{WithMergedBlocksStore(mergedBlocksStore, maxKeptFinalBlocks)})
	return fh, mergedBlocksStore
}

func processBlocks(t *testing.T, fh *ForkableHub, blocks []*pbbstream.Block) {
	t.Helper()
	for _, blk := range blocks {
		require.NoError(t, fh.ProcessBlock(blk, nil))
	}
}

func TestForkableHub_KeepsFinalBlocksUntilMerged(t *testing.T) {
	fh, mergedBlocksStore := newMergedBlocksTestHub(t, testChain(300, 510), []uint64{100, 200, 300}, 2000)
	require.NoError(t, fh.bootstrap())
	require.Equal(t, uint64(300), fh.forkable.LowestBlockNum())

	// LIB reaches 898 while the last merged-blocks file still ends at 399
	processBlocks(t, fh, testChain(511, 900))
	require.NoError(t, fh.mergedBlocks.advance(context.Background(), fh.forkable.LIBNum()))
	assert.Equal(t, uint64(300), fh.forkable.LowestBlockNum(), "blocks of the last merged-blocks file and above are kept")
	assert.Equal(t, uint64(499), fh.mergedBlocks.lag(fh.forkable.LIBNum()))

	var fromLastMergedFile []*bstream.PreprocessedBlock
	require.NoError(t, fh.forkable.CallWithBlocksFromNum(399, func(blocks []*bstream.PreprocessedBlock) { fromLastMergedFile = blocks }, false))
	require.NotEmpty(t, fromLastMergedFile)
	assert.Equal(t, uint64(399), fromLastMergedFile[0].Block.Number)

	// the merger catches up
	setMergedBlocksFiles(mergedBlocksStore, 400, 500, 600)
	require.NoError(t, fh.mergedBlocks.advance(context.Background(), fh.forkable.LIBNum()))
	processBlocks(t, fh, testChain(901, 901))
	assert.Equal(t, uint64(600), fh.forkable.LowestBlockNum(), "blocks below the last merged-blocks file are dropped")
	assert.Equal(t, uint64(200), fh.mergedBlocks.lag(fh.forkable.LIBNum()))

	// the hub follows the merged-blocks files as they show up
	setMergedBlocksFiles(mergedBlocksStore, 700)
	processBlocks(t, fh, testChain(902, 1010))
	require.NoError(t, fh.mergedBlocks.advance(context.Background(), fh.forkable.LIBNum()))
	processBlocks(t, fh, testChain(1011, 1011))
	assert.Equal(t, uint64(700), fh.forkable.LowestBlockNum())

	setMergedBlocksFiles(mergedBlocksStore, 800, 900)
	require.NoError(t, fh.mergedBlocks.advance(context.Background(), fh.forkable.LIBNum()))
	processBlocks(t, fh, testChain(1012, 1012))
	assert.Equal(t, uint64(900), fh.forkable.LowestBlockNum())
}

func TestForkableHub_MaxKeptFinalBlocks(t *testing.T) {
	fh, _ := newMergedBlocksTestHub(t, testChain(300, 510), []uint64{300}, 350)
	require.NoError(t, fh.bootstrap())

	processBlocks(t, fh, testChain(511, 900))
	assert.Equal(t, uint64(898-350), fh.forkable.LowestBlockNum())
}

func TestForkableHub_BootstrapFromLastMergedBlocksFile(t *testing.T) {
	fh, _ := newMergedBlocksTestHub(t, testChain(100, 900), []uint64{100, 200, 300}, 2000)
	require.NoError(t, fh.bootstrap())

	assert.Equal(t, uint64(300), fh.forkable.LowestBlockNum(), "starts from the last merged-blocks file")
	assert.Equal(t, uint64(400), fh.mergedBlocks.nextBase.Load())
}

func TestForkableHub_BootstrapWithMergedBlocksUpToDate(t *testing.T) {
	fh, mergedBlocksStore := newMergedBlocksTestHub(t, testChain(100, 900), []uint64{100, 200, 300, 400, 500, 600, 700}, 2000)

	var lookups []string
	mergedBlocksStore.FileExistsFunc = func(_ context.Context, base string) (bool, error) {
		lookups = append(lookups, base)
		_, found := mergedBlocksStore.Files[base]
		return found, nil
	}

	require.NoError(t, fh.bootstrap())
	assert.Equal(t, uint64(700), fh.forkable.LowestBlockNum())
	assert.Equal(t, []string{"0000000800", "0000000700"}, lookups)

	// no lookup until LIB passes the end of the next file
	require.NoError(t, fh.mergedBlocks.advance(context.Background(), 899))
	assert.Equal(t, []string{"0000000800", "0000000700"}, lookups)

	require.NoError(t, fh.mergedBlocks.advance(context.Background(), 900))
	assert.Equal(t, []string{"0000000800", "0000000700", "0000000800"}, lookups)
	assert.Equal(t, uint64(800), fh.mergedBlocks.nextBase.Load())
}

func TestMergedBlocksTracker_LocateStopsAtMaxKeptFinalBlocks(t *testing.T) {
	store := dstore.NewMockStore(nil)
	tracker := &mergedBlocksTracker{store: store, bundleSize: 100, maxKeptFinalBlocks: 500}

	lowest, err := tracker.locate(context.Background(), 4800, 5010)
	require.NoError(t, err)
	assert.Equal(t, uint64(4600), lowest)
	assert.Equal(t, uint64(4600), tracker.nextBase.Load())
}

func TestForkableHub_LiveBootstrapFromLastMergedBlocksFile(t *testing.T) {
	fh, _ := newMergedBlocksTestHub(t, testChain(100, 900), []uint64{100, 200, 300}, 2000)

	// no bootstrap from one-block files: the hub starts from its first live block
	processBlocks(t, fh, testChain(901, 901))

	require.True(t, fh.IsReady())
	assert.Equal(t, uint64(300), fh.forkable.LowestBlockNum(), "starts from the last merged-blocks file instead of the live block's LIB")
	assert.Equal(t, uint64(400), fh.mergedBlocks.nextBase.Load())
}

func TestMergedBlocksTracker_RetriesFailedLookups(t *testing.T) {
	prev := bootstrapRetryDelay
	bootstrapRetryDelay = time.Millisecond
	defer func() { bootstrapRetryDelay = prev }()

	store := dstore.NewMockStore(nil)
	setMergedBlocksFiles(store, 300)

	failures := 0
	store.FileExistsFunc = func(_ context.Context, base string) (bool, error) {
		if failures > 0 {
			failures--
			return false, errors.New("store unavailable")
		}
		_, found := store.Files[base]
		return found, nil
	}

	tracker := &mergedBlocksTracker{store: store, bundleSize: 100, maxKeptFinalBlocks: 2000}

	failures = bootstrapMaxAttempts - 1
	lowest, err := tracker.locate(context.Background(), 600, 800)
	require.NoError(t, err)
	assert.Equal(t, uint64(300), lowest)

	failures = bootstrapMaxAttempts
	_, err = tracker.locate(context.Background(), 600, 800)
	require.ErrorContains(t, err, "store unavailable")
}

func TestForkableHub_MergedBlocksUpTo(t *testing.T) {
	var nilHub *ForkableHub
	_, ok := nilHub.MergedBlocksUpTo()
	assert.False(t, ok)

	fh, _ := newMergedBlocksTestHub(t, testChain(100, 900), []uint64{100, 200, 300}, 2000)
	_, ok = fh.MergedBlocksUpTo()
	assert.False(t, ok, "not known before the hub looked")

	require.NoError(t, fh.bootstrap())
	upTo, ok := fh.MergedBlocksUpTo()
	assert.True(t, ok)
	assert.Equal(t, uint64(400), upTo)
}

func TestMergedBlocksTracker_PurgeLimit(t *testing.T) {
	tracker := &mergedBlocksTracker{bundleSize: 100, maxKeptFinalBlocks: 2000}

	assert.Equal(t, uint64(0), tracker.purgeLimit(1500, 1500), "nothing dropped before the merged blocks are located")
	assert.Equal(t, uint64(3000), tracker.purgeLimit(5000, 5000), "except past maxKeptFinalBlocks")

	tracker.nextBase.Store(4800)
	tracker.known.Store(true)
	assert.Equal(t, uint64(4700), tracker.purgeLimit(5000, 5000), "keeps the last merged-blocks file")
	assert.Equal(t, uint64(4600), tracker.purgeLimit(5000, 4600), "never above the forkable's own cutoff")
}

func TestForkableHub_BootstrapWhenMergedBlocksStoreFails(t *testing.T) {
	prev := bootstrapRetryDelay
	bootstrapRetryDelay = time.Millisecond
	defer func() { bootstrapRetryDelay = prev }()

	fh, mergedBlocksStore := newMergedBlocksTestHub(t, testChain(100, 900), []uint64{100, 200, 300}, 2000)
	mergedBlocksStore.FileExistsFunc = func(context.Context, string) (bool, error) {
		return false, errors.New("store unavailable")
	}

	require.NoError(t, fh.bootstrap())
	assert.Equal(t, uint64(600), fh.forkable.LowestBlockNum(), "two merged-blocks files below LIB")

	processBlocks(t, fh, testChain(901, 1200))
	assert.Equal(t, uint64(600), fh.forkable.LowestBlockNum(), "nothing is dropped while the merged blocks are not located")
}
