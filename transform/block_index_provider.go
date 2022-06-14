package transform

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type BitmapGetter interface {
	Get(string) *roaring64.Bitmap
	GetByPrefix(string) *roaring64.Bitmap
	GetBySuffix(string) *roaring64.Bitmap
}

// GenericBlockIndexProvider responds to queries on BlockIndex
type GenericBlockIndexProvider struct {
	// currentIndex represents the currently loaded BlockIndex
	currentIndex *blockIndex

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
		indexOpsTimeout:    15 * time.Second,
		indexShortname:     indexShortname,
		filterFunc:         filterFunc,
		possibleIndexSizes: possibleIndexSizes,
		store:              store,
	}
}

// WithinRange determines the existence of an index which includes the provided blockNum
// it also attempts to pre-emptively load the index (read-ahead)
func (ip *GenericBlockIndexProvider) WithinRange(ctx context.Context, blockNum uint64) bool {
	ctx, cancel := context.WithTimeout(ctx, ip.indexOpsTimeout)
	defer cancel()

	if ip.currentIndex != nil && ip.currentIndex.lowBlockNum <= blockNum && (ip.currentIndex.lowBlockNum+ip.currentIndex.indexSize) > blockNum {
		return true
	}

	r, lowBlockNum, indexSize := ip.findIndexContaining(ctx, blockNum)
	if r == nil {
		return false
	}
	if err := ip.loadIndex(r, lowBlockNum, indexSize); err != nil {
		zlog.Error("couldn't load index", zap.Error(err))
		return false
	}
	return true
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
// if such a file exists, returns an io.Reader; nil otherwise
func (ip *GenericBlockIndexProvider) findIndexContaining(ctx context.Context, blockNum uint64) (r io.ReadCloser, lowBlockNum, indexSize uint64) {
	for _, size := range ip.possibleIndexSizes {
		var err error

		base := lowBoundary(blockNum, size)
		filename := toIndexFilename(size, base, ip.indexShortname)

		r, err = ip.store.OpenObject(ctx, filename)
		if err == dstore.ErrNotFound {
			zlog.Debug("couldn't find index file",
				zap.String("filename", filename),
				zap.Uint64("blockNum", blockNum),
			)
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			zlog.Error("couldn't open index from dstore", zap.Error(err))
			continue
		}

		return r, base, size
	}

	return
}

// loadIndex will populate the indexProvider's currentIndex from the provided io.Reader
func (ip *GenericBlockIndexProvider) loadIndex(r io.ReadCloser, lowBlockNum, indexSize uint64) error {
	obj, err := ioutil.ReadAll(r)
	if err != nil {
		return fmt.Errorf("couldn't read index: %s", err)
	}
	if err := r.Close(); err != nil {
		return fmt.Errorf("cannot properly close index file %d: %w", lowBlockNum, err)
	}

	newIdx := NewBlockIndex(lowBlockNum, indexSize)
	err = newIdx.unmarshal(obj)
	if err != nil {
		return fmt.Errorf("couldn't unmarshal index: %s", err)
	}

	ip.currentIndex = newIdx

	// the user-provided function identifies the blockNums of interest
	ip.currentMatchingBlocks = ip.filterFunc(ip.currentIndex)

	return nil
}

// loadRange will traverse available indexes until it finds the desired blockNum
func (ip *GenericBlockIndexProvider) loadRange(ctx context.Context, blockNum uint64) error {
	if ip.currentIndex != nil && blockNum >= ip.currentIndex.lowBlockNum && blockNum < ip.currentIndex.lowBlockNum+ip.currentIndex.indexSize {
		return nil
	}

	// truncate any prior matching blocks
	ip.currentMatchingBlocks = []uint64{}

	ctx, cancel := context.WithTimeout(ctx, ip.indexOpsTimeout)
	defer cancel()

	r, lowBlockNum, indexSize := ip.findIndexContaining(ctx, blockNum)
	if r == nil {
		return fmt.Errorf("couldn't find index containing block_num: %d", blockNum)
	}
	if err := ip.loadIndex(r, lowBlockNum, indexSize); err != nil {
		return err
	}

	return nil
}
