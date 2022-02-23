package transform

import (
	"github.com/streamingfast/dstore"
	"time"
)

// BlockIndexProvider responds to queries on BlockIndex
type BlockIndexProvider struct {
	// currentIndex represents the currently loaded BlockIndex
	currentIndex *BlockIndex

	// currentMatchingBlocks contains the block numbers matching a specific filterFunc
	currentMatchingBlocks []uint64

	// filterFunc is a user-defined function which scours currentIndex to populate currentMatchingBlocks
	filterFunc func(index *BlockIndex) (matchingBlocks []uint64)

	// indexOpsTimeout is the time after which Index operations will timeout
	indexOpsTimeout time.Duration

	// possibleIndexSizes is a list of possible sizes each BlockIndex file can have
	possibleIndexSizes []uint64

	// store represents the dstore.Store where the index files live
	store dstore.Store
}

// NewBlockIndexProvider initializes and returns a new BlockIndexProvider
func NewBlockIndexProvider(
	store dstore.Store,
	filterFunc func(index *BlockIndex) (matchingBlocks []uint64),
) *BlockIndexProvider {
	// @todo(froch, 20220223): firm up what the possibleIndexSizes can be
	possibleIndexSizes := []uint64{100000, 10000, 1000, 100}
	return &BlockIndexProvider{
		store:              store,
		indexOpsTimeout:    15 * time.Second,
		filterFunc:         filterFunc,
		possibleIndexSizes: possibleIndexSizes,
	}
}
