package hub

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/streamingfast/dstore"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withShortBootstrapRetryDelay shrinks bootstrapRetryDelay for the duration of a test,
// so exercising the retry loop doesn't slow the suite down.
func withShortBootstrapRetryDelay(t *testing.T) {
	t.Helper()
	original := bootstrapRetryDelay
	bootstrapRetryDelay = time.Millisecond
	t.Cleanup(func() { bootstrapRetryDelay = original })
}

// TestForkableHub_Bootstrap_RetriesTransientlyGappedListing simulates a Walk that races
// with the merger deleting the one-block files of a bundle it just merged: the first
// listing comes back with a gap in the middle (a file momentarily missing), and later
// listings are complete. Bootstrap must retry rather than giving up on the torn snapshot.
func TestForkableHub_Bootstrap_RetriesTransientlyGappedListing(t *testing.T) {
	withShortBootstrapRetryDelay(t)

	blocks := []*pbbstream.Block{
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
	}

	lsf := bstream.NewTestSourceFactory()
	testOneBlockStore := dstore.NewMockStore(nil)
	fh := NewForkableHub(lsf.NewSource, 0, testOneBlockStore)

	AddToMockStore(t, testOneBlockStore, blocks...)

	allFilenames := make([]string, len(blocks))
	for i, blk := range blocks {
		allFilenames[i] = bstream.BlockFileName(blk)
	}
	gappedFilename := bstream.BlockFileName(blocks[8]) // "00000123", the file racing with deletion

	var walkCalls int
	testOneBlockStore.WalkFunc = func(ctx context.Context, prefix string, f func(filename string) error) error {
		walkCalls++
		for _, fn := range allFilenames {
			if walkCalls == 1 && fn == gappedFilename {
				continue
			}
			if err := f(fn); err != nil {
				return err
			}
		}
		return nil
	}

	err := fh.bootstrap()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, walkCalls, 2, "bootstrap should have retried after the gapped first listing")
	assert.Equal(t, blocks[7].Number, fh.forkable.LowestBlockNum()) // "00000122", same as the sunny-path test using this block set
}

// TestForkableHub_Bootstrap_RetriesWhenFileVanishesMidDecode simulates a file that was
// present when Walk listed it but is gone (dstore.ErrNotFound) by the time it is opened
// for decoding a moment later, which is expected during the merger's delete race.
func TestForkableHub_Bootstrap_RetriesWhenFileVanishesMidDecode(t *testing.T) {
	withShortBootstrapRetryDelay(t)

	blocks := []*pbbstream.Block{
		bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
		bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
		bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
		bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
		bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
	}

	lsf := bstream.NewTestSourceFactory()
	testOneBlockStore := dstore.NewMockStore(nil)
	fh := NewForkableHub(lsf.NewSource, 0, testOneBlockStore)

	AddToMockStore(t, testOneBlockStore, blocks...)

	vanishingFilename := bstream.BlockFileName(blocks[1]) // "00000004"
	var openedOnce bool
	testOneBlockStore.OpenObjectFunc = func(ctx context.Context, name string) (io.ReadCloser, error) {
		if name == vanishingFilename && !openedOnce {
			openedOnce = true
			return nil, dstore.ErrNotFound
		}
		content, ok := testOneBlockStore.Files[name]
		require.True(t, ok, "unexpected file requested: %s", name)
		return io.NopCloser(bytes.NewReader(content)), nil
	}

	err := fh.bootstrap()
	require.NoError(t, err)
	assert.Equal(t, uint64(3), fh.forkable.LowestBlockNum())
}

// TestForkableHub_Run_SurvivesPermanentGapInOneBlockListing reproduces the reported
// production race end to end: the most recent one-block file's chain has a gap it can
// never close (the file is truly gone, not a transient listing artifact), so
// bootstrapping from the one-block store fails even after retries. The hub must fall
// back to live blocks with a fresh forkable instead of keeping whatever partial state
// the failed bootstrap attempt produced, and must not treat the first unlinkable live
// block as a fatal "reconnection".
func TestForkableHub_Run_SurvivesPermanentGapInOneBlockListing(t *testing.T) {
	withShortBootstrapRetryDelay(t)

	// 3-4-5-8-9 is a complete, linkable chain (LIB lands on 3, as in the "sunny path"
	// bootstrap test). 120-121-122-124 is a second one-block file batch whose file for
	// block 123 is permanently gone (merged and deleted), so the most recent file (124)
	// never links back to anything: bootstrap fails the same way as the existing "most
	// recent block not linkable" test, on every retry, since the file never reappears.
	oneBlocks := []*pbbstream.Block{
		bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
		bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
		bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
		bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
		bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
		bstream.TestBlockWithLIBNum("00000120", "00000119", 288),
		bstream.TestBlockWithLIBNum("00000121", "00000120", 288),
		bstream.TestBlockWithLIBNum("00000122", "00000121", 288),
		bstream.TestBlockWithLIBNum("00000124", "00000123", 290),
	}

	lsf := bstream.NewTestSourceFactory()
	testOneBlockStore := dstore.NewMockStore(nil)
	fh := NewForkableHub(lsf.NewSource, 0, testOneBlockStore)
	AddToMockStore(t, testOneBlockStore, oneBlocks...)

	go fh.Run()

	select {
	case <-fh.Ready:
		t.Fatal("hub should not become ready from a one-block chain with a permanent gap")
	case <-time.After(500 * time.Millisecond):
	}

	ls := <-lsf.Created

	// The live block's LibNum (3) becomes the starting LIB for a cold-start bootstrap,
	// same as it would for a hub that never found any one-block files at all. A buggy
	// hub would instead still be sitting on whatever LIB the failed bootstrap attempt
	// left behind, and would either mislink or die trying to reconcile it.
	require.NoError(t, ls.Push(bstream.TestBlockWithLIBNum("00000010", "00000009", 3), nil))

	select {
	case <-fh.Ready:
	case <-time.After(time.Second):
		t.Fatal("hub never became ready from live blocks after bootstrap failed")
	}

	_, headID, _, _, err := fh.forkable.HeadInfo()
	require.NoError(t, err)
	assert.Equal(t, "00000010", headID)
}

// TestForkableHub_ProcessBlock_ReconnectionCheckNotFatalBeforeReady isolates fix #3: the
// same shape that trips the "cannot link block after reconnection" fatal error (a live
// block's LibNum matches a one-block file's number, and that file is not linkable to the
// current LIB) must not be fatal when the hub was never marked ready. Before readiness
// there was never a reconnection to speak of, only the pre-fix race where a failed
// bootstrap left stale forkdb state that a live block then couldn't reconcile.
func TestForkableHub_ProcessBlock_ReconnectionCheckNotFatalBeforeReady(t *testing.T) {
	lsf := bstream.NewTestSourceFactory()
	testOneBlockStore := dstore.NewMockStore(nil)

	fh := NewForkableHub(lsf.NewSource, 0, testOneBlockStore)

	AddToMockStore(t, testOneBlockStore,
		bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
		bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
		bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
		bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
	)
	require.NoError(t, fh.bootstrap())
	require.False(t, fh.IsReady(), "bootstrap succeeding does not by itself make the hub ready")

	// "00000010" hex-decodes to block number 16 (0x10): it collides with the live
	// block's LibNum below, which is what triggers the reconnection check.
	AddToMockStore(t, testOneBlockStore,
		bstream.TestBlockWithLIBNum("00000010", "00000009", 8),
		bstream.TestBlockWithLIBNum("00000011b", "00000010", 8),
	)

	err := fh.ProcessBlock(bstream.TestBlockWithLIBNum("00000012", "00000011a", 0x10), nil)
	require.NoError(t, err, "before readiness, an unlinkable block after a one-block lookup must not be fatal")
	assert.False(t, fh.IsReady())
}
