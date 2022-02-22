package transform

import (
	"bytes"
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"io/ioutil"
	"time"
)

// BlockIndexer creates and performs I/O operations on index files
type BlockIndexer struct {
	// currentIndex represents the currently loaded index
	currentIndex *BlockIndex
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

// KV exposes the contents of the currently loaded index
func (i *BlockIndexer) KV() map[string]*roaring64.Bitmap {
	if i.currentIndex == nil {
		return nil
	}
	return i.currentIndex.KV()
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
		}
	}

	// upper bound reached
	if blockNum >= i.currentIndex.LowBlockNum()+i.indexSize {
		if err := i.WriteIndex(); err != nil {
			zlog.Warn("couldn't write index", zap.Error(err))
		}
		lb := lowBoundary(blockNum, i.indexSize)
		i.currentIndex = NewBlockIndex(lb, i.indexSize)
	}

	for _, key := range keys {
		i.currentIndex.Add(key, blockNum)
	}
}

// WriteIndex writes the BlockIndexer's currentIndex to a file in the active dstore.Store
func (i *BlockIndexer) WriteIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), i.indexOpsTimeout)
	defer cancel()

	if i.currentIndex == nil {
		zlog.Warn("attempted to write nil index")
		return nil
	}

	data, err := i.currentIndex.Marshal()
	if err != nil {
		return err
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

// ReadIndex attempts to load the BlockIndexer's currentIndex from the provided indexName in the current dstore.Store
func (i *BlockIndexer) ReadIndex(indexName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), i.indexOpsTimeout)
	defer cancel()

	if i.currentIndex == nil {
		i.currentIndex = NewBlockIndex(0, i.indexSize)
	}

	dstoreObj, err := i.store.OpenObject(ctx, indexName)
	if err != nil {
		return fmt.Errorf("couldn't open object %s from dstore: %s", indexName, err)
	}

	obj, err := ioutil.ReadAll(dstoreObj)
	if err != nil {
		return fmt.Errorf("couldn't read %s: %s", indexName, err)
	}

	err = i.currentIndex.Unmarshal(obj)
	if err != nil {
		return fmt.Errorf("couldn't unmarshal %s: %s", indexName, err)
	}

	return nil
}
