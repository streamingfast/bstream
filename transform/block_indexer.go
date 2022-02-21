package transform

import (
	"fmt"
	"github.com/streamingfast/dstore"
	"time"
)

// BlockIndexer creates and performs I/O operations on index files
type BlockIndexer struct {
	currentIndex    *BlockIndex
	store           dstore.Store
	indexSize       uint64
	indexOpsTimeout time.Duration
}

func NewBlockIndexer(store dstore.Store, indexSize uint64) *BlockIndexer {
	return &BlockIndexer{
		store:           store,
		currentIndex:    nil,
		indexSize:       indexSize,
		indexOpsTimeout: 15 * time.Second,
	}
}

// String returns a summary of the current BlockIndexer
func (i *BlockIndexer) String() string {
	if i.currentIndex == nil {
		return fmt.Sprintf("size: %d, kv: nil", i.indexSize)
	}
	return fmt.Sprintf("size: %d, kv: %d", i.indexSize, len(i.currentIndex.kv))
}
