package hub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

// mergedBlocksPollInterval is how often the hub checks whether the next merged-blocks file
// showed up. It is a var so tests can shrink it.
var mergedBlocksPollInterval = 5 * time.Second

// mergedBlocksBehindLogInterval is how often the hub warns that it keeps extra final
// blocks because merged blocks are behind.
const mergedBlocksBehindLogInterval = 30 * time.Second

// WithMergedBlocksStore makes the hub keep its final blocks until they are available in a
// merged-blocks file of store: it holds every block from the start of the last
// merged-blocks file it found, so a stream reading the files can always continue on the
// hub. The hub is then built with a keepFinalBlocks of 0, which only matters to a hub
// without this option: a fixed depth below LIB leaves, when the merger falls further
// behind than that depth, a range of blocks that neither the files nor the hub can serve.
//
// The hub learns about new files with one FileExists call on the next expected file, at
// most every 5 seconds and only once LIB passed the end of that file.
//
// maxKeptFinalBlocks limits the final blocks held in memory when the merger is behind or
// the store cannot be reached. Past it the oldest blocks are dropped even though no
// merged-blocks file holds them. It must cover at least two merged-blocks files, which is
// what the hub holds when the merger keeps up.
func WithMergedBlocksStore(store dstore.Store, maxKeptFinalBlocks int) Option {
	return func(h *ForkableHub) {
		h.mergedBlocks = &mergedBlocksTracker{store: store, maxKeptFinalBlocks: uint64(maxKeptFinalBlocks)}
	}
}

// WithMergedBlocksLagMetric reports how many blocks LIB is ahead of the last block found
// in the merged-blocks store. It needs WithMergedBlocksStore. With a merger keeping up,
// the value moves between 1 and a little over one merged-blocks file; it keeps growing
// when the merger is stopped or too slow.
func WithMergedBlocksLagMetric(metric forkable.Uint64Metric) Option {
	return func(h *ForkableHub) {
		h.mergedBlocksLagMetric = metric
	}
}

// mergedBlocksTracker follows the merged-blocks store from the hub's point of view: which
// is the first file, going up, that is not there yet.
type mergedBlocksTracker struct {
	store              dstore.Store
	bundleSize         uint64
	maxKeptFinalBlocks uint64

	lookupLock sync.Mutex // serializes the store lookups

	// nextBase is the base block number of the first merged-blocks file not seen in the
	// store. Every file below it was seen, or sits under where the hub started looking.
	nextBase atomic.Uint64
	known    atomic.Bool
}

func mergedBlocksFilename(base uint64) string {
	return fmt.Sprintf("%010d", base)
}

// fileExists tells whether the merged-blocks file starting at base is in the store. A
// lookup that fails is retried like a bootstrap attempt is: bootstrapMaxAttempts times,
// bootstrapRetryDelay apart. A missing file is an answer, not a failure.
func (t *mergedBlocksTracker) fileExists(ctx context.Context, base uint64) (exists bool, err error) {
	for attempt := range bootstrapMaxAttempts {
		if attempt > 0 {
			time.Sleep(bootstrapRetryDelay)
		}
		if exists, err = t.store.FileExists(ctx, mergedBlocksFilename(base)); err == nil {
			return exists, nil
		}
	}
	return false, fmt.Errorf("looking up merged-blocks file %s: %w", mergedBlocksFilename(base), err)
}

// locate finds where the merged blocks end, looking down from the file that holds
// lowestBlockNum, and returns the block the hub must start from for its blocks to overlap
// the last merged-blocks file. That block is never further than maxKeptFinalBlocks below
// libNum, nor below the first streamable block.
func (t *mergedBlocksTracker) locate(ctx context.Context, lowestBlockNum, libNum uint64) (uint64, error) {
	t.lookupLock.Lock()
	defer t.lookupLock.Unlock()

	firstBase := t.firstBase()
	base := max(lowestBlockNum/t.bundleSize*t.bundleSize, firstBase)
	nextBase := base

	for {
		exists, err := t.fileExists(ctx, base)
		if err != nil {
			return 0, err
		}
		if exists {
			nextBase = base + t.bundleSize
			break
		}
		if base <= firstBase {
			// no merged-blocks file yet
			nextBase = firstBase
			break
		}
		if libNum-min(libNum, base)+t.bundleSize > t.maxKeptFinalBlocks {
			// The merged blocks end lower than the hub can go. Where they end still
			// matters to the lag metric and to MergedBlocksUpTo.
			if nextBase, err = t.searchEnd(ctx, firstBase, base); err != nil {
				return 0, err
			}
			break
		}
		base -= t.bundleSize
	}

	t.nextBase.Store(nextBase)
	t.known.Store(true)

	return max(min(base, lowestBlockNum), bstream.GetProtocolFirstStreamableBlock), nil
}

// firstBase is the base of the merged-blocks file holding the first streamable block.
func (t *mergedBlocksTracker) firstBase() uint64 {
	return bstream.GetProtocolFirstStreamableBlock / t.bundleSize * t.bundleSize
}

// searchEnd returns the base of the first missing merged-blocks file, knowing that the one
// at missing is not there and assuming the files are contiguous from firstBase. It probes
// further and further down until it finds a file, then narrows down between the two.
func (t *mergedBlocksTracker) searchEnd(ctx context.Context, firstBase, missing uint64) (uint64, error) {
	found := firstBase
	for step := t.bundleSize; ; step *= 2 {
		probe := firstBase
		if missing-firstBase > step {
			probe = missing - step
		}

		exists, err := t.fileExists(ctx, probe)
		if err != nil {
			return 0, err
		}
		if exists {
			found = probe
			break
		}
		if probe == firstBase {
			return firstBase, nil
		}
		missing = probe
	}

	for missing-found > t.bundleSize {
		middle := found + (missing-found)/t.bundleSize/2*t.bundleSize
		exists, err := t.fileExists(ctx, middle)
		if err != nil {
			return 0, err
		}
		if exists {
			found = middle
		} else {
			missing = middle
		}
	}
	return missing, nil
}

// advance moves nextBase over the files that showed up. A file is only looked up once LIB
// passed its last block, since the merger cannot have written it before.
func (t *mergedBlocksTracker) advance(ctx context.Context, libNum uint64) error {
	t.lookupLock.Lock()
	defer t.lookupLock.Unlock()

	for {
		base := t.nextBase.Load()
		if libNum < base+t.bundleSize {
			return nil
		}

		exists, err := t.fileExists(ctx, base)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		t.nextBase.Store(base + t.bundleSize)
	}
}

// lag is how many blocks libNum is ahead of the last merged block: the final blocks that
// are not in a merged-blocks file.
func (t *mergedBlocksTracker) lag(libNum uint64) uint64 {
	// before the first merged-blocks file, the lag counts from the first streamable block
	firstUnmerged := max(t.nextBase.Load(), bstream.GetProtocolFirstStreamableBlock)
	if libNum < firstUnmerged {
		return 0
	}
	return libNum + 1 - firstUnmerged
}

// purgeLimit is a forkable.WithFinalBlocksPurgeLimit function. It keeps the blocks of the
// last merged-blocks file and everything above: a stream reading the files joins the hub
// on a block it received from a file, so the hub has to overlap the last one.
func (t *mergedBlocksTracker) purgeLimit(libNum, cutoff uint64) uint64 {
	// Nothing is dropped on account of merged blocks until the hub knows where they end.
	var limit uint64
	if nextBase := t.nextBase.Load(); t.known.Load() && nextBase > t.bundleSize {
		limit = nextBase - t.bundleSize
	}
	if libNum > t.maxKeptFinalBlocks {
		limit = max(limit, libNum-t.maxKeptFinalBlocks)
	}
	return min(limit, cutoff)
}

// MergedBlocksUpTo returns the first block number the hub has not found in a merged-blocks
// file: every block below it can be read from the merged-blocks store. ok is false when
// the hub does not follow that store (see WithMergedBlocksStore) or has not looked yet.
func (h *ForkableHub) MergedBlocksUpTo() (blockNum uint64, ok bool) {
	if h == nil || h.mergedBlocks == nil || !h.mergedBlocks.known.Load() {
		return 0, false
	}
	return h.mergedBlocks.nextBase.Load(), true
}

// trackMergedBlocks keeps the merged blocks tracker and the lag metric up to date until
// the hub terminates.
func (h *ForkableHub) trackMergedBlocks() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := time.NewTicker(mergedBlocksPollInterval)
	defer ticker.Stop()

	var lastBehindLog time.Time
	for {
		select {
		case <-h.Terminating():
			return
		case <-ticker.C:
		}

		if !h.IsReady() {
			continue
		}

		t := h.mergedBlocks
		libNum := h.forkable.LIBNum()

		if !t.known.Load() {
			if _, err := t.locate(ctx, h.forkable.LowestBlockNum(), libNum); err != nil {
				h.logger.Warn("cannot find the last merged-blocks file", zap.Error(err))
				continue
			}
		}

		if err := t.advance(ctx, libNum); err != nil {
			h.logger.Warn("cannot check for new merged-blocks files", zap.Error(err))
		}

		lag := t.lag(libNum)
		if h.mergedBlocksLagMetric != nil {
			h.mergedBlocksLagMetric.SetUint64(lag)
		}

		// a merger keeping up is at most one file behind LIB, plus the time it takes to write it
		if lag > 3*t.bundleSize && time.Since(lastBehindLog) >= mergedBlocksBehindLogInterval {
			lastBehindLog = time.Now()
			lowest := h.forkable.LowestBlockNum()
			h.logger.Warn("merged blocks are behind, keeping more final blocks in memory to cover for them",
				zap.Uint64("lib_num", libNum),
				zap.Uint64("next_merged_blocks_file", t.nextBase.Load()),
				zap.Uint64("merged_blocks_lag", lag),
				zap.Uint64("lowest_block_num", lowest),
				zap.Uint64("max_kept_final_blocks", t.maxKeptFinalBlocks),
				zap.Bool("blocks_missing_between_merged_and_hub", lowest > t.nextBase.Load()),
			)
		}
	}
}
