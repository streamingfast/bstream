package transform

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type BitmapGetter func(string) *roaring64.Bitmap

// GenericBlockIndexProvider responds to queries on BlockIndex
type GenericBlockIndexProvider struct {
	// currentIndex represents the currently loaded BlockIndex
	currentIndex *blockIndex

	lazyIndexManager *lazyIndexManager

	// currentMatchingBlocks contains the block numbers matching a specific filterFunc
	currentMatchingBlocks []uint64

	// filterFunc is a user-defined function which scours currentIndex to populate currentMatchingBlocks
	filterFunc func(BitmapGetter) (matchingBlocks []uint64)

	// indexOpsTimeout is the time after which Index operations will timeout
	indexOpsTimeout time.Duration

	// indexShortname is a shorthand identifier for the type of index being manipulated
	indexShortname string

	// possibleIndexSizes is a list of possible sizes each BlockIndex file can have
	// A nil value here will default to sane Streamingfast assumptions
	possibleIndexSizes []uint64

	// store represents the dstore.Store where the index files live
	store dstore.Store
}

type lazyIndexManager struct {
	sync.RWMutex
	refreshMutex  sync.Mutex
	storedIndexes []*lazyBlockIndex

	possibleIndexSizes []uint64
	indexShortname     string
	store              dstore.Store
	lastRefresh        time.Time
	refreshTTL         time.Duration
}

// refresh will repopulate its list of indexes from files only if TTL since last refresh is expired
func (m *lazyIndexManager) refresh(ctx context.Context, lowBlock uint64) {
	m.refreshMutex.Lock()
	defer m.refreshMutex.Unlock()
	if time.Since(m.lastRefresh) < m.refreshTTL {
		return
	}
	m.lastRefresh = time.Now()

	sizes := make(map[uint64]bool)
	for _, size := range m.possibleIndexSizes {
		sizes[size] = true
	}

	startFile := toIndexFilename(0, lowBlock, m.indexShortname) // 00012345.0 is before 00012345.500
	var indexes []*lazyBlockIndex

	err := m.store.WalkFrom(ctx, "", startFile, func(filename string) error {
		size, blockNum, short, err := parseIndexFilename(filename)
		if err != nil {
			zlog.Warn("parsing index files", zap.Error(err), zap.String("filename", filename))
			return nil
		}
		if !sizes[size] {
			return nil
		}
		if short != m.indexShortname {
			return nil
		}

		// handle same range served by indexes of different size (bigger size wins)
		for i, idx := range indexes {
			if idx.contains(blockNum) {
				if idx.indexSize > size { // ex: we had 0.10000, which contains 1000.1000, skip this last one
					return nil
				}
				indexes = append(indexes[0:i], newLazyBlockIndex(blockNum, size)) // [0.10, 10.10, 20.10] sees 20.20, replaces by [0.10, 10.10, 20.20]
				return nil
			}
		}

		m.RLock()
		defer m.RUnlock()
		// handle indexes that were already in our 'nextIndexes' store, keep them because some may be pre-loaded
		for _, idx := range m.storedIndexes {
			if idx.lowBlockNum == blockNum && idx.indexSize == size {
				indexes = append(indexes, idx)
				return nil
			}
		}

		// just create a new lazyIndex with this file
		indexes = append(indexes, newLazyBlockIndex(blockNum, size))
		return nil
	})
	if err != nil {
		if err := ctx.Err(); err != nil {
			zlog.Debug("context error while refreshing", zap.Error(err))
			return
		}
		zlog.Warn("error while refreshing indexes", zap.Error(err))
	}

	m.Lock()
	defer m.Unlock()
	m.storedIndexes = indexes
	if len(m.storedIndexes) > 0 {
		lastIdx := m.storedIndexes[len(m.storedIndexes)-1]

		zlog.Debug("preloaded indexes",
			zap.Uint64("low_block_num", m.storedIndexes[0].lowBlockNum),
			zap.Uint64("highest_block_num", lastIdx.lowBlockNum+lastIdx.indexSize),
			zap.Int("length", len(m.storedIndexes)),
		)
	}
}

func (m *lazyIndexManager) containing(blockNum uint64) *lazyBlockIndex {
	m.Lock()
	defer m.Unlock()

	for i, idx := range m.storedIndexes {
		if idx.contains(blockNum) {
			// preload this one
			go idx.load(context.Background(), m.store, m.indexShortname)

			// preload next one
			if len(m.storedIndexes) > i+1 {
				go m.storedIndexes[i+1].load(context.Background(), m.store, m.indexShortname)
			}

			zlog.Debug("returning index from lazy index manager")
			return idx
		}
	}
	zlog.Debug("did not find index from lazy index manager", zap.Uint64("block_num", blockNum))
	return nil
}

// NewGenericBlockIndexProvider initializes and returns a new GenericBlockIndexProvider
func NewGenericBlockIndexProvider(
	store dstore.Store,
	indexShortname string,
	possibleIndexSizes []uint64,
	filterFunc func(BitmapGetter) []uint64,
) *GenericBlockIndexProvider {

	// @todo(froch, 20220223): firm up what the possibleIndexSizes can be
	if possibleIndexSizes == nil {
		possibleIndexSizes = []uint64{100000, 10000, 1000, 100}
	}

	return &GenericBlockIndexProvider{
		lazyIndexManager: &lazyIndexManager{
			indexShortname:     indexShortname,
			possibleIndexSizes: possibleIndexSizes,
			store:              store,
			refreshTTL:         10 * time.Second,
		},
		indexOpsTimeout:    15 * time.Second,
		indexShortname:     indexShortname,
		filterFunc:         filterFunc,
		possibleIndexSizes: possibleIndexSizes,
		store:              store,
	}
}

// WithinRange determines the existence of an index which includes the provided blockNum
func (ip *GenericBlockIndexProvider) WithinRange(ctx context.Context, blockNum uint64) bool {
	go ip.lazyIndexManager.refresh(ctx, blockNum)

	timeoutCtx, cancel := context.WithTimeout(ctx, ip.indexOpsTimeout)
	defer cancel()

	if ip.currentIndex.contains(blockNum) {
		return true
	}

	return ip.findIndexContaining(timeoutCtx, blockNum) != nil
}

// Matches returns true if the provided blockNum matches entries in the index
func (ip *GenericBlockIndexProvider) Matches(ctx context.Context, blockNum uint64) (bool, error) {
	if err := ip.loadRange(ctx, blockNum); err != nil {
		return false, fmt.Errorf("couldn't load range: %s", err)
	}

	for _, matchingBlock := range ip.currentMatchingBlocks {
		if blockNum == matchingBlock {
			return true, nil
		}
	}

	return false, nil
}

// NextMatching attempts to find the next matching blockNum which matches the provided filter.
// It can determine if a match is found within the bounds of the known index, or outside those bounds.
// If no match corresponds to the filter, it will return the highest available blockNum
func (ip *GenericBlockIndexProvider) NextMatching(ctx context.Context, blockNum uint64, exclusiveUpTo uint64) (num uint64, passedIndexBoundary bool, err error) {
	go ip.lazyIndexManager.refresh(ctx, blockNum)

	if err = ip.loadRange(ctx, blockNum); err != nil {
		return 0, false, fmt.Errorf("couldn't load range: %s", err)
	}

	for {
		for _, block := range ip.currentMatchingBlocks {
			if block > blockNum {
				return block, false, nil
			}
		}

		nextBaseBlock := ip.currentIndex.lowBlockNum + ip.currentIndex.indexSize
		if exclusiveUpTo != 0 && nextBaseBlock >= exclusiveUpTo {
			return exclusiveUpTo, false, nil
		}
		err = ip.loadRange(ctx, nextBaseBlock)
		if err != nil {
			return nextBaseBlock, true, nil
		}
	}
}

// findIndexContaining tries to find an index file in dstore containing the provided blockNum
func (ip *GenericBlockIndexProvider) findIndexContaining(ctx context.Context, blockNum uint64) *lazyBlockIndex {
	if idx := ip.lazyIndexManager.containing(blockNum); idx != nil {
		return idx
	}

	for _, size := range ip.possibleIndexSizes {
		var err error

		base := lowBoundary(blockNum, size)
		filename := toIndexFilename(size, base, ip.indexShortname)

		exists, err := ip.store.FileExists(ctx, filename)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			zlog.Error("couldn't open index from dstore", zap.Error(err))
			continue
		}
		if exists {
			return newLazyBlockIndex(base, size)
		}
	}
	return nil
}

// loadRange will traverse available indexes until it finds the desired blockNum
func (ip *GenericBlockIndexProvider) loadRange(ctx context.Context, blockNum uint64) error {
	if ip.currentIndex.contains(blockNum) {
		return nil
	}

	// truncate any prior matching blocks
	ip.currentMatchingBlocks = []uint64{}

	ctx, cancel := context.WithTimeout(ctx, ip.indexOpsTimeout)
	defer cancel()

	if lazyIndex := ip.findIndexContaining(ctx, blockNum); lazyIndex != nil {
		idx, err := lazyIndex.load(ctx, ip.store, ip.indexShortname)
		if err != nil {
			return err
		}
		ip.currentIndex = idx
		ip.currentMatchingBlocks = ip.filterFunc(ip.currentIndex.Get)
		return nil
	}
	return fmt.Errorf("range not found for block %d", blockNum)
}
