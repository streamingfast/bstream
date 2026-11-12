package hub

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

// mergedBlocksLookupTimeout bounds each search of the merged-blocks store, so a store call
// that hangs does not stop the hub from following the merger. It is a var so tests can
// shrink it.
var mergedBlocksLookupTimeout = 30 * time.Second

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
// The hub finds where merged blocks end with one listing of the store when it starts. It
// then learns about new files with one FileExists call on the next expected file, at most
// every 5 seconds and only once LIB passed the end of that file, or with one listing when
// LIB is far enough for several new files to be there, as when the merger catches up.
//
// bundleSize is the number of blocks per merged-blocks file in store (0 means
// bstream.DefaultMergedBlocksBundleSize).
//
// maxKeptFinalBlocks limits the final blocks held in memory when the merger is behind or
// the store cannot be reached. Past it the oldest blocks are dropped even though no
// merged-blocks file holds them. It must cover at least two merged-blocks files, which is
// what the hub holds when the merger keeps up.
func WithMergedBlocksStore(store dstore.Store, bundleSize uint64, maxKeptFinalBlocks int) Option {
	return func(h *ForkableHub) {
		h.mergedBlocks = &mergedBlocksTracker{
			store:              store,
			bundleSize:         bstream.SanitizeBundleSize(bundleSize),
			maxKeptFinalBlocks: uint64(maxKeptFinalBlocks),
		}
	}
}

// WithMergedBlocksLagMetric reports how many blocks LIB is ahead of the last block found
// in the merged-blocks store. It needs WithMergedBlocksStore. With a merger keeping up,
// the value moves between 1 and a little over one merged-blocks file; it keeps growing
// when the merger is stopped or too slow. The hub does not look for merged-blocks files
// more than maxKeptFinalBlocks below LIB, so a hub starting while the merger is further
// behind reports about maxKeptFinalBlocks at first.
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
	// store. Every file below it was seen, or sits under where the hub started looking:
	// the hub never looks more than maxKeptFinalBlocks below LIB.
	nextBase atomic.Uint64
	known    atomic.Bool

	// lookupFailedAt is when the hub last failed to locate the merged blocks while
	// starting, in Unix nanoseconds.
	lookupFailedAt atomic.Int64
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
			select {
			case <-ctx.Done():
				return false, fmt.Errorf("looking up merged-blocks file %s: %w", mergedBlocksFilename(base), ctx.Err())
			case <-time.After(bootstrapRetryDelay):
			}
		}
		if exists, err = t.store.FileExists(ctx, mergedBlocksFilename(base)); err == nil {
			return exists, nil
		}
	}
	return false, fmt.Errorf("looking up merged-blocks file %s: %w", mergedBlocksFilename(base), err)
}

// firstRunFrom lists the store from the merged-blocks file starting at base, and returns
// the first run of consecutive files it holds there: the base of its first file and the
// base that follows its last one. ok is false when the store holds no file from base on.
//
// It is one listing whatever the number of files, stopped at the end of the run. A listing
// that fails is retried like a file lookup is.
func (t *mergedBlocksTracker) firstRunFrom(ctx context.Context, base uint64) (first, next uint64, ok bool, err error) {
	for attempt := range bootstrapMaxAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, 0, false, fmt.Errorf("listing merged-blocks files from %s: %w", mergedBlocksFilename(base), ctx.Err())
			case <-time.After(bootstrapRetryDelay):
			}
		}

		ok = false
		err = t.store.WalkFrom(ctx, "", mergedBlocksFilename(base), func(filename string) error {
			name, _, _ := strings.Cut(filename, ".")
			fileBase, parseErr := strconv.ParseUint(name, 10, 64)
			if parseErr != nil || fileBase%t.bundleSize != 0 || fileBase < base {
				return nil // not a merged-blocks file of this store's size
			}
			if !ok {
				first, next, ok = fileBase, fileBase+t.bundleSize, true
				return nil
			}
			if fileBase != next {
				return dstore.StopIteration
			}
			next += t.bundleSize
			return nil
		})
		if err == nil {
			return first, next, ok, nil
		}
	}
	return 0, 0, false, fmt.Errorf("listing merged-blocks files from %s: %w", mergedBlocksFilename(base), err)
}

// locate finds where the merged blocks end and returns the block the hub must start from
// at a LIB of libNum for its blocks to overlap the last merged-blocks file. That block is
// never further than maxKeptFinalBlocks below libNum, nor below the first streamable
// block, and neither is where locate looks.
//
// It takes the first run of consecutive files in that range, so a file the merger wrote
// ahead of a lower one that is still missing is not taken for the end of merged blocks.
func (t *mergedBlocksTracker) locate(ctx context.Context, libNum uint64) (uint64, error) {
	t.lookupLock.Lock()
	defer t.lookupLock.Unlock()

	// the lowest file the hub can start from
	floor := t.firstBase()
	if reach := libNum + t.bundleSize; reach > t.maxKeptFinalBlocks {
		floor = max(floor, (reach-t.maxKeptFinalBlocks-1)/t.bundleSize*t.bundleSize)
	}

	_, next, ok, err := t.firstRunFrom(ctx, floor)
	if err != nil {
		return 0, err
	}

	// Without a file in reach, either there is none yet, or the merged blocks end lower
	// than the hub can go: the merger writes files in order, so it writes the one at floor
	// once it gets there.
	start, nextBase := floor, floor
	if ok {
		start, nextBase = next-t.bundleSize, next
	}

	t.nextBase.Store(nextBase)
	t.known.Store(true)

	return max(start, bstream.GetProtocolFirstStreamableBlock), nil
}

// knownStart is the block the hub must start from at a LIB of libNum, computed from what
// the last successful locate found instead of searching the store: the start of the last
// merged-blocks file, never further than maxKeptFinalBlocks below libNum, nor below the
// first streamable block. ok is false when no locate succeeded yet.
func (t *mergedBlocksTracker) knownStart(libNum uint64) (start uint64, ok bool) {
	if !t.known.Load() {
		return 0, false
	}

	start = t.firstBase()
	if nextBase := t.nextBase.Load(); nextBase >= start+t.bundleSize {
		start = nextBase - t.bundleSize
	}
	if libNum > t.maxKeptFinalBlocks {
		start = max(start, libNum-t.maxKeptFinalBlocks)
	}
	return max(start, bstream.GetProtocolFirstStreamableBlock), true
}

// lookupFailedRecently reports whether locating the merged blocks failed less than
// mergedBlocksPollInterval ago.
func (t *mergedBlocksTracker) lookupFailedRecently() bool {
	failedAt := t.lookupFailedAt.Load()
	return failedAt != 0 && time.Since(time.Unix(0, failedAt)) < mergedBlocksPollInterval
}

// firstBase is the base of the merged-blocks file holding the first streamable block.
func (t *mergedBlocksTracker) firstBase() uint64 {
	return bstream.GetProtocolFirstStreamableBlock / t.bundleSize * t.bundleSize
}

// advance moves nextBase over the files that showed up. A file is only looked up once LIB
// passed its last block, since the merger cannot have written it before.
func (t *mergedBlocksTracker) advance(ctx context.Context, libNum uint64) error {
	t.lookupLock.Lock()
	defer t.lookupLock.Unlock()

	base := t.nextBase.Load()
	if libNum < base+t.bundleSize {
		return nil
	}

	// only the next file can be there: one lookup
	if libNum < base+2*t.bundleSize {
		exists, err := t.fileExists(ctx, base)
		if err != nil {
			return err
		}
		if exists {
			t.nextBase.Store(base + t.bundleSize)
		}
		return nil
	}

	// several files can be there, as when the merger catches up: one listing
	first, next, ok, err := t.firstRunFrom(ctx, base)
	if err != nil {
		return err
	}
	if ok && first == base {
		t.nextBase.Store(next)
	}
	return nil
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
// file, at most maxKeptFinalBlocks below LIB when the hub started: the files end there or
// lower. ok is false when the hub does not follow that store (see WithMergedBlocksStore)
// or has not looked yet.
func (h *ForkableHub) MergedBlocksUpTo() (blockNum uint64, ok bool) {
	if h == nil || h.mergedBlocks == nil || !h.mergedBlocks.known.Load() {
		return 0, false
	}
	return h.mergedBlocks.nextBase.Load(), true
}

// followMergedBlocks locates the merged blocks if no search succeeded yet, then moves
// over the files that showed up. It returns false when the merged blocks are not located.
func (h *ForkableHub) followMergedBlocks(ctx context.Context, libNum uint64) bool {
	ctx, cancel := context.WithTimeout(ctx, mergedBlocksLookupTimeout)
	defer cancel()

	t := h.mergedBlocks
	if !t.known.Load() {
		if _, err := t.locate(ctx, libNum); err != nil {
			h.logger.Warn("cannot find the last merged-blocks file", zap.Error(err))
			return false
		}
	}

	if err := t.advance(ctx, libNum); err != nil {
		h.logger.Warn("cannot check for new merged-blocks files", zap.Error(err))
	}
	return true
}

// trackMergedBlocks keeps the merged blocks tracker and the lag metric up to date until
// the hub terminates.
func (h *ForkableHub) trackMergedBlocks() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-h.Terminating()
		cancel()
	}()

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

		if !h.followMergedBlocks(ctx, libNum) {
			continue
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
