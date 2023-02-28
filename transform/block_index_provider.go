package transform

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type BitmapGetter interface {
	Get(string) *roaring64.Bitmap
	GetByPrefixAndSuffix(prefix string, suffix string) *roaring64.Bitmap
}

// GenericBlockIndexProvider responds to queries on BlockIndex
type GenericBlockIndexProvider struct {
	sync.Mutex

	ctx context.Context

	loadedLowBoundary           uint64
	loadedExclusiveHighBoundary uint64

	matchingBlocks []uint64

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
		ctx:                context.Background(),
		indexOpsTimeout:    60 * time.Second,
		indexShortname:     indexShortname,
		filterFunc:         filterFunc,
		possibleIndexSizes: possibleIndexSizes,
		store:              store,
	}
}

func (ip *GenericBlockIndexProvider) BlocksInRange(baseBlock, bundleSize uint64) (out []uint64, err error) {
	if baseBlock%bundleSize != 0 {
		return nil, fmt.Errorf("blocks_in_range called not on boundary")
	}
	if err = ip.loadRange(baseBlock, bundleSize); err != nil {
		return nil, fmt.Errorf("cannot load range: %s", err)
	}
	exclusiveUpperBound := baseBlock + bundleSize

	if baseBlock < bstream.GetProtocolFirstStreamableBlock {
		baseBlock = bstream.GetProtocolFirstStreamableBlock
	}

	ip.Lock()
	defer ip.Unlock()
	for _, block := range ip.matchingBlocks {
		if block < baseBlock {
			continue
		}
		if block > exclusiveUpperBound {
			break
		}
		out = append(out, block)
	}
	return
}

func (ip *GenericBlockIndexProvider) loadRange(blockNum, bundleSize uint64) error {
	if blockNum >= ip.loadedLowBoundary && blockNum+bundleSize <= ip.loadedExclusiveHighBoundary {
		return nil // range already loaded
	}

	ctx, cancel := context.WithTimeout(ip.ctx, ip.indexOpsTimeout)
	defer cancel()

	r, lowBlockNum, indexSize := ip.findIndexContaining(ctx, blockNum, bundleSize)
	if r == nil {
		return fmt.Errorf("couldn't find index containing block_num: %d", blockNum)
	}

	idx, err := ReadNewBlockIndex(r)
	if err != nil {
		return err
	}

	ip.matchingBlocks = ip.filterFunc(idx)
	ip.loadedLowBoundary = lowBlockNum
	ip.loadedExclusiveHighBoundary = lowBlockNum + indexSize

	return nil
}

// findIndexContaining tries to find an index file in dstore containing the provided blockNum
// if such a file exists, returns an io.Reader; nil otherwise
func (ip *GenericBlockIndexProvider) findIndexContaining(ctx context.Context, blockNum, bundleSize uint64) (r io.ReadCloser, lowBlockNum, indexSize uint64) {
	for _, size := range ip.possibleIndexSizes {
		if size < bundleSize {
			continue // never use indexes smaller than bundleSize
		}
		var err error

		base := lowBoundary(blockNum, size)
		filename := toIndexFilename(size, base, ip.indexShortname)

		r, err = ip.store.OpenObject(ctx, filename)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			zlog.Debug("couldn't open index from dstore", zap.Error(err), zap.String("filename", filename))
			continue
		}

		return r, base, size
	}

	return
}
