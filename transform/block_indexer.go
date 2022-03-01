package transform

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

// BlockIndexer creates and performs I/O operations on index files
type BlockIndexer struct {
	// currentIndex represents the currently loaded index
	currentIndex *blockIndex

	// indexSize is the distance between upper and lower bounds of the currentIndex
	indexSize uint64

	// indexShortname is a shorthand identifier for the type of index being manipulated
	indexShortname string

	// indexOpsTimeout is the time after which Index operations will timeout
	indexOpsTimeout time.Duration

	// store represents the dstore.Store where the index files live
	store dstore.Store
}

// NewBlockIndexer initializes and returns a new BlockIndexer
func NewBlockIndexer(store dstore.Store, indexSize uint64, indexShortname string) *BlockIndexer {
	if indexShortname == "" {
		indexShortname = "default"
	}

	return &BlockIndexer{
		currentIndex:    nil,
		indexSize:       indexSize,
		indexShortname:  indexShortname,
		indexOpsTimeout: 15 * time.Second,
		store:           store,
	}
}

// String returns a summary of the current BlockIndexer
func (i *BlockIndexer) String() string {
	if i.currentIndex == nil {
		return fmt.Sprintf("indexSize: %d, len(kv): nil", i.indexSize)
	}
	return fmt.Sprintf("indexSize: %d, len(kv): %d", i.indexSize, len(i.currentIndex.kv))
}

// Add will populate the BlockIndexer's currentIndex
// by adding the specified BlockNum to the bitmaps identified with the provided keys
func (i *BlockIndexer) Add(keys []string, blockNum uint64) {
	// init lower bound
	if i.currentIndex == nil {
		switch {

		case blockNum%i.indexSize == 0:
			// we're on a boundary
			i.currentIndex = NewBlockIndex(blockNum, i.indexSize)

		case blockNum == bstream.GetProtocolFirstStreamableBlock:
			// handle offset
			lb := lowBoundary(blockNum, i.indexSize)
			i.currentIndex = NewBlockIndex(lb, i.indexSize)

		default:
			zlog.Warn("couldn't determine boundary for block", zap.Uint64("blk_num", blockNum))
			return
		}
	}

	// upper bound reached
	if blockNum >= i.currentIndex.lowBlockNum+i.indexSize {
		if err := i.writeIndex(); err != nil {
			zlog.Warn("couldn't write index", zap.Error(err))
		}
		lb := lowBoundary(blockNum, i.indexSize)
		i.currentIndex = NewBlockIndex(lb, i.indexSize)
	}

	for _, key := range keys {
		i.currentIndex.add(key, blockNum)
	}
}

// writeIndex writes the BlockIndexer's currentIndex to a file in the active dstore.Store
func (i *BlockIndexer) writeIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), i.indexOpsTimeout)
	defer cancel()

	if i.currentIndex == nil {
		return fmt.Errorf("attempted to write a nil index")
	}

	data, err := i.currentIndex.marshal()
	if err != nil {
		return fmt.Errorf("couldn't marshal the current index: %w", err)
	}

	filename := toIndexFilename(i.indexSize, i.currentIndex.lowBlockNum, i.indexShortname)
	if err = i.store.WriteObject(ctx, filename, bytes.NewReader(data)); err != nil {
		zlog.Warn("cannot write index file to store",
			zap.String("filename", filename),
			zap.Error(err),
		)
	}
	zlog.Info("wrote file to store",
		zap.String("filename", filename),
		zap.Uint64("low_block_num", i.currentIndex.lowBlockNum),
	)

	return nil
}
