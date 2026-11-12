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

// defaultMaxKeptFinalBlocksFactor sets the default limit on the final blocks a hub keeps
// while merged blocks are behind, as a multiple of its keepFinalBlocks.
const defaultMaxKeptFinalBlocksFactor = 10

// mergedBlocksPollInterval is how often the hub checks whether the next merged-blocks file
// showed up. It is a var so tests can shrink it.
var mergedBlocksPollInterval = 5 * time.Second

// mergedBlocksBehindLogInterval is how often the hub warns that it keeps extra final
// blocks because merged blocks are behind.
const mergedBlocksBehindLogInterval = 30 * time.Second

// WithMergedBlocksStore makes the hub keep its final blocks until they are available in a
// merged-blocks file of store, instead of dropping everything older than keepFinalBlocks
// below LIB. Without it, a merger falling more than keepFinalBlocks behind LIB leaves a
// range of blocks that neither the merged-blocks files nor the hub can serve, and every
// stream reading through the files waits there until the merger catches up.
//
// The hub learns about new files with one FileExists call on the next expected file, at
// most every 5 seconds and only once LIB passed the end of that file.
//
// The extra blocks are held in memory, up to WithMaxKeptFinalBlocks.
func WithMergedBlocksStore(store dstore.Store) Option {
	return func(h *ForkableHub) {
		h.mergedBlocks = &mergedBlocksTracker{store: store}
	}
}

// WithMaxKeptFinalBlocks limits how many final blocks the hub keeps below LIB while it
// waits for merged blocks, see WithMergedBlocksStore. Past that limit the oldest blocks
// are dropped even though no merged-blocks file holds them. Defaults to 10 times the
// hub's keepFinalBlocks; a value below keepFinalBlocks is raised to it.
func WithMaxKeptFinalBlocks(count int) Option {
	return func(h *ForkableHub) {
		h.maxKeptFinalBlocks = count
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
// the last merged-blocks file. It does not go further than maxKeptFinalBlocks below libNum.
func (t *mergedBlocksTracker) locate(ctx context.Context, lowestBlockNum, libNum uint64) (uint64, error) {
	t.lookupLock.Lock()
	defer t.lookupLock.Unlock()

	firstBase := bstream.GetProtocolFirstStreamableBlock / t.bundleSize * t.bundleSize
	base := lowestBlockNum / t.bundleSize * t.bundleSize

	for {
		exists, err := t.fileExists(ctx, base)
		if err != nil {
			return 0, err
		}
		if exists {
			t.nextBase.Store(base + t.bundleSize)
			break
		}
		if base <= firstBase || libNum-min(libNum, base)+t.bundleSize > t.maxKeptFinalBlocks {
			t.nextBase.Store(base)
			break
		}
		base -= t.bundleSize
	}
	t.known.Store(true)

	return max(min(base, lowestBlockNum), bstream.GetProtocolFirstStreamableBlock), nil
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

// lag is how many blocks libNum is ahead of the last merged block.
func (t *mergedBlocksTracker) lag(libNum uint64) uint64 {
	nextBase := t.nextBase.Load()
	if libNum < nextBase {
		return 0
	}
	return libNum + 1 - nextBase
}

// purgeLimit is a forkable.WithFinalBlocksPurgeLimit function. It keeps the blocks of the
// last merged-blocks file and everything above: a stream reading the files joins the hub
// on a block it received from a file, so the hub has to overlap the last one.
func (t *mergedBlocksTracker) purgeLimit(libNum, cutoff uint64) uint64 {
	if !t.known.Load() {
		return cutoff
	}

	var limit uint64
	if nextBase := t.nextBase.Load(); nextBase > t.bundleSize {
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

		if lag > uint64(h.keepFinalBlocks) && time.Since(lastBehindLog) >= mergedBlocksBehindLogInterval {
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
