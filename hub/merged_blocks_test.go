package hub

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	fh := NewForkableHubWithOptions(lsf.NewSource, 0, oneBlocksStore, []Option{WithMergedBlocksStore(mergedBlocksStore, 100, maxKeptFinalBlocks)})
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

func TestForkableHub_MergedBlocksBundleSize(t *testing.T) {
	require.Equal(t, uint64(100), bstream.DefaultMergedBlocksBundleSize)

	prev := bstream.GetProtocolFirstStreamableBlock
	bstream.GetProtocolFirstStreamableBlock = 1
	t.Cleanup(func() { bstream.GetProtocolFirstStreamableBlock = prev })

	oneBlocksStore := dstore.NewMockStore(nil)
	AddToMockStore(t, oneBlocksStore, testChain(2000, 3100)...)
	mergedBlocksStore := dstore.NewMockStore(nil)
	setMergedBlocksFiles(mergedBlocksStore, 0, 1000, 2000)

	fh := NewForkableHubWithOptions(bstream.NewTestSourceFactory().NewSource, 0, oneBlocksStore, []Option{WithMergedBlocksStore(mergedBlocksStore, 1000, 20000)})
	require.NoError(t, fh.bootstrap())
	assert.Equal(t, uint64(2000), fh.forkable.LowestBlockNum())
	upTo, ok := fh.MergedBlocksUpTo()
	require.True(t, ok)
	assert.Equal(t, uint64(3000), upTo)

	setMergedBlocksFiles(mergedBlocksStore, 3000)
	processBlocks(t, fh, testChain(3101, 4010))
	require.NoError(t, fh.mergedBlocks.advance(context.Background(), fh.forkable.LIBNum()))
	processBlocks(t, fh, testChain(4011, 4011))

	upTo, _ = fh.MergedBlocksUpTo()
	assert.Equal(t, uint64(4000), upTo)
	assert.Equal(t, uint64(3000), fh.forkable.LowestBlockNum())
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
	bases := func(from, to uint64) (out []uint64) {
		for base := from; base <= to; base += 100 {
			out = append(out, base)
		}
		return out
	}

	tests := []struct {
		name                 string
		firstStreamableBlock uint64
		mergedBases          []uint64
		expectedLowest       uint64
		expectedNextBase     uint64
	}{
		{"no merged-blocks file at all", 0, nil, 4600, 4600},
		{"no merged-blocks file at all, chain starting above 0", 1250, nil, 4600, 4600},
		{"only the first file", 0, bases(0, 0), 4600, 4600},
		{"merged blocks end far below", 0, bases(0, 1200), 4600, 4600},
		{"merged blocks end far below, chain starting above 0", 1250, bases(1200, 3300), 4600, 4600},
		{"merged blocks end one file below reach", 0, bases(0, 4500), 4600, 4600},
		{"merged blocks end within reach", 0, bases(0, 4600), 4600, 4700},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prev := bstream.GetProtocolFirstStreamableBlock
			bstream.GetProtocolFirstStreamableBlock = test.firstStreamableBlock
			defer func() { bstream.GetProtocolFirstStreamableBlock = prev }()

			store := dstore.NewMockStore(nil)
			setMergedBlocksFiles(store, test.mergedBases...)
			lookups := 0
			store.FileExistsFunc = func(_ context.Context, base string) (bool, error) {
				lookups++
				_, found := store.Files[base]
				return found, nil
			}

			tracker := &mergedBlocksTracker{store: store, bundleSize: 100, maxKeptFinalBlocks: 500}

			lowest, err := tracker.locate(context.Background(), 4800, 5010)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLowest, lowest)
			assert.Equal(t, test.expectedNextBase, tracker.nextBase.Load())
			assert.LessOrEqual(t, lookups, 3, "stops looking at maxKeptFinalBlocks below LIB")
		})
	}
}

func TestMergedBlocksTracker_BeforeFirstMergedBlocksFile(t *testing.T) {
	prev := bstream.GetProtocolFirstStreamableBlock
	bstream.GetProtocolFirstStreamableBlock = 150
	defer func() { bstream.GetProtocolFirstStreamableBlock = prev }()

	store := dstore.NewMockStore(nil)
	tracker := &mergedBlocksTracker{store: store, bundleSize: 100, maxKeptFinalBlocks: 2000}

	lowest, err := tracker.locate(context.Background(), 150, 180)
	require.NoError(t, err)
	assert.Equal(t, uint64(150), lowest, "never below the first streamable block")
	assert.Equal(t, uint64(100), tracker.nextBase.Load())
	assert.Equal(t, uint64(31), tracker.lag(180), "counted from the first streamable block")
	assert.Equal(t, uint64(0), tracker.purgeLimit(450, 450), "everything is kept until the first file exists")

	// the first file holds blocks 150 to 199
	setMergedBlocksFiles(store, 100)
	require.NoError(t, tracker.advance(context.Background(), 199))
	assert.Equal(t, uint64(100), tracker.nextBase.Load(), "not looked up before LIB passed its end")
	require.NoError(t, tracker.advance(context.Background(), 205))
	assert.Equal(t, uint64(200), tracker.nextBase.Load())
	assert.Equal(t, uint64(6), tracker.lag(205))
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

// liveChainWithoutLIB returns blocks from..to whose LIB is 100 blocks below them, so a hub
// receiving only these blocks has no LIB in its forkDB.
func liveChainWithoutLIB(from, to uint64) (out []*pbbstream.Block) {
	id := func(num uint64) string { return fmt.Sprintf("%08x", num) }
	for num := from; num <= to; num++ {
		out = append(out, bstream.TestBlockWithLIBNum(id(num), id(num-1), num-100))
	}
	return out
}

func countFileExists(store *dstore.MockStore, fail bool) *int {
	calls := 0
	store.FileExistsFunc = func(_ context.Context, base string) (bool, error) {
		calls++
		if fail {
			return false, errors.New("store unavailable")
		}
		_, found := store.Files[base]
		return found, nil
	}
	return &calls
}

func TestForkableHub_LiveLinkingSearchesMergedBlocksOnce(t *testing.T) {
	fh, mergedBlocksStore := newMergedBlocksTestHub(t, nil, []uint64{100, 200, 300}, 2000)
	calls := countFileExists(mergedBlocksStore, false)

	processBlocks(t, fh, liveChainWithoutLIB(901, 901))
	require.False(t, fh.forkable.ForkDBHasLib())
	searchCalls := *calls
	require.NotZero(t, searchCalls)

	processBlocks(t, fh, liveChainWithoutLIB(902, 950))
	require.False(t, fh.forkable.ForkDBHasLib())
	assert.Equal(t, searchCalls, *calls, "later blocks reuse what the first search found")
	assert.Equal(t, uint64(300), func() uint64 { start, _ := fh.mergedBlocks.knownStart(850); return start }())
}

func TestForkableHub_LiveLinkingWaitsAfterFailedSearch(t *testing.T) {
	prev := bootstrapRetryDelay
	bootstrapRetryDelay = time.Millisecond
	defer func() { bootstrapRetryDelay = prev }()

	fh, mergedBlocksStore := newMergedBlocksTestHub(t, nil, []uint64{100, 200, 300}, 2000)
	calls := countFileExists(mergedBlocksStore, true)

	processBlocks(t, fh, liveChainWithoutLIB(901, 950))
	assert.Equal(t, bootstrapMaxAttempts, *calls, "one failed search, then none until mergedBlocksPollInterval passed")

	fh.mergedBlocks.lookupFailedAt.Store(time.Now().Add(-mergedBlocksPollInterval).UnixNano())
	processBlocks(t, fh, liveChainWithoutLIB(951, 951))
	assert.Equal(t, 2*bootstrapMaxAttempts, *calls, "searches again once mergedBlocksPollInterval passed")
}

func TestMergedBlocksTracker_KnownStart(t *testing.T) {
	prev := bstream.GetProtocolFirstStreamableBlock
	bstream.GetProtocolFirstStreamableBlock = 1
	defer func() { bstream.GetProtocolFirstStreamableBlock = prev }()

	tracker := &mergedBlocksTracker{bundleSize: 100, maxKeptFinalBlocks: 2000}
	_, ok := tracker.knownStart(800)
	assert.False(t, ok, "not known before a search")

	tracker.known.Store(true)
	tracker.nextBase.Store(0)
	start, _ := tracker.knownStart(800)
	assert.Equal(t, uint64(1), start, "no merged-blocks file: first streamable block")

	tracker.nextBase.Store(400)
	start, _ = tracker.knownStart(800)
	assert.Equal(t, uint64(300), start, "start of the last merged-blocks file")

	start, _ = tracker.knownStart(5000)
	assert.Equal(t, uint64(3000), start, "never more than maxKeptFinalBlocks below LIB")
}

func TestMergedBlocksTracker_FollowsMergerFromFarBehind(t *testing.T) {
	prev := bstream.GetProtocolFirstStreamableBlock
	bstream.GetProtocolFirstStreamableBlock = 0
	defer func() { bstream.GetProtocolFirstStreamableBlock = prev }()

	// the store does not start at the first streamable block, and the merger is far behind
	store := dstore.NewMockStore(nil)
	for base := uint64(10000); base <= 10400; base += 100 {
		setMergedBlocksFiles(store, base)
	}
	tracker := &mergedBlocksTracker{store: store, bundleSize: 100, maxKeptFinalBlocks: 2000}

	lowest, err := tracker.locate(context.Background(), 20010, 20010)
	require.NoError(t, err)
	assert.Equal(t, uint64(18100), lowest)
	assert.Equal(t, uint64(18100), tracker.nextBase.Load(), "looks no further than maxKeptFinalBlocks below LIB")

	// the merger catches up, writing its files in order
	for base := uint64(10500); base <= 19900; base += 100 {
		setMergedBlocksFiles(store, base)
	}
	require.NoError(t, tracker.advance(context.Background(), 20010))
	assert.Equal(t, uint64(20000), tracker.nextBase.Load())
	assert.Equal(t, uint64(11), tracker.lag(20010))
}

func TestForkableHub_TrackerRetryLooksDownFromLIB(t *testing.T) {
	prev := bootstrapRetryDelay
	bootstrapRetryDelay = time.Millisecond
	defer func() { bootstrapRetryDelay = prev }()
	prevPoll := mergedBlocksPollInterval
	mergedBlocksPollInterval = 10 * time.Millisecond
	defer func() { mergedBlocksPollInterval = prevPoll }()

	fh, mergedBlocksStore := newMergedBlocksTestHub(t, testChain(100, 900), []uint64{100, 200, 300}, 2000)
	failing := true
	var lookedLock sync.Mutex
	var looked []string
	mergedBlocksStore.FileExistsFunc = func(_ context.Context, base string) (bool, error) {
		if failing {
			return false, errors.New("store unavailable")
		}
		lookedLock.Lock()
		defer lookedLock.Unlock()
		looked = append(looked, base)
		_, found := mergedBlocksStore.Files[base]
		return found, nil
	}

	require.NoError(t, fh.bootstrap())
	close(fh.Ready)
	failing = false
	go fh.trackMergedBlocks()
	defer fh.Shutdown(nil)

	require.Eventually(t, func() bool { _, ok := fh.MergedBlocksUpTo(); return ok }, time.Second, 5*time.Millisecond)
	upTo, _ := fh.MergedBlocksUpTo()
	assert.Equal(t, uint64(400), upTo)
	lookedLock.Lock()
	defer lookedLock.Unlock()
	assert.Equal(t, mergedBlocksFilename(800), looked[0], "starts from LIB's file")
}
