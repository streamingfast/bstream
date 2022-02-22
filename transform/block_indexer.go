package transform

import (
	"bytes"
	"context"
	"fmt"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
	"io/ioutil"
	"time"
)

// BlockIndexer creates and performs I/O operations on index files
type BlockIndexer struct {
	// CurrentIndex represents the currently loaded index
	CurrentIndex *BlockIndex
	// IndexSize represents the size between upper and lower bounds of the current index
	IndexSize uint64
	// IndexShortname represents the type of index being manipulated
	IndexShortname string

	// indexOpsTimeout represents the time after which Index operations will timeout
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
		CurrentIndex:    nil,
		IndexSize:       indexSize,
		IndexShortname:  indexShortname,
		indexOpsTimeout: 15 * time.Second,
		store:           store,
	}
}

// String returns a summary of the current BlockIndexer
func (i *BlockIndexer) String() string {
	if i.CurrentIndex == nil {
		return fmt.Sprintf("size: %d, kv: nil", i.IndexSize)
	}
	return fmt.Sprintf("size: %d, kv: %d", i.IndexSize, len(i.CurrentIndex.kv))
}

// WriteIndex writes the BlockIndexer's CurrentIndex to a file in the active dstore.Store
func (i *BlockIndexer) WriteIndex() error {
	ctx, cancel := context.WithTimeout(context.Background(), i.indexOpsTimeout)
	defer cancel()

	if i.CurrentIndex == nil {
		zlog.Warn("attempted to write nil index")
		return nil
	}

	data, err := i.CurrentIndex.Marshal()
	if err != nil {
		return err
	}

	filename := toIndexFilename(i.IndexSize, i.CurrentIndex.lowBlockNum, i.IndexShortname)
	if err = i.store.WriteObject(ctx, filename, bytes.NewReader(data)); err != nil {
		zlog.Warn("cannot write index file to store",
			zap.String("filename", filename),
			zap.Error(err),
		)
	}
	zlog.Info("wrote file to store",
		zap.String("filename", filename),
		zap.Uint64("low_block_num", i.CurrentIndex.lowBlockNum),
	)

	return nil
}

// ReadIndex attempts to load the BlockIndexer's CurrentIndex from the provided indexName in the current dstore.Store
func (i *BlockIndexer) ReadIndex(indexName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), i.indexOpsTimeout)
	defer cancel()

	if i.CurrentIndex == nil {
		i.CurrentIndex = NewBlockIndex(0, i.IndexSize)
	}

	dstoreObj, err := i.store.OpenObject(ctx, indexName)
	if err != nil {
		return fmt.Errorf("couldn't open object %s from dstore: %s", indexName, err)
	}

	obj, err := ioutil.ReadAll(dstoreObj)
	if err != nil {
		return fmt.Errorf("couldn't read %s: %s", indexName, err)
	}

	err = i.CurrentIndex.Unmarshal(obj)
	if err != nil {
		return fmt.Errorf("couldn't unmarshal %s: %s", indexName, err)
	}

	return nil
}
