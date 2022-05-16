package bstream

import (
	"fmt"
	"os"
	"time"

	"github.com/streamingfast/atm"
)

type CacheBytesFunc func(*Block, []byte) (GetBytesFunc, error)
type GetBytesFunc func() ([]byte, error)

// FIXME(abourget): this isn't needed, because passing a nil `CacheBytesFunc` will produce the same result
func NewMemoryBytesFunc(data []byte) (GetBytesFunc, error) {
	return func() ([]byte, error) {
		return data, nil
	}, nil
}

type DiskCache struct {
	atmCache    *atm.Cache
	namespace   string // "raw_blocks" or some hash of substreams, to distinguish the types of payloads, concatenated with the block_id
}

func NewDiskCache(cachePath string, namespace string, maxRecentEntryBytes int, maxEntryByAgeBytes int) (*DiskCache, error) {
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		err := os.Mkdir(cachePath, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("mkdir for disk cache: %w", err)
		}
	}

	atmCache, err := atm.NewInitializedCache(cachePath, maxRecentEntryBytes, maxEntryByAgeBytes, atm.NewFileIO())
	if err != nil {
		return nil, fmt.Errorf("initialize atm cache: %w", err)
	}

	return &DiskCache{
		atmCache:    atmCache,
		namespace:   namespace,
	}, nil
}

func (c *DiskCache) CacheBytesFunc(block *Block, data []byte) (GetBytesFunc, error) {
	blockID := block.Id
	blockTime := block.Timestamp
	blockNum := block.Number
	key := c.namespace + ":" + blockID
	_, err := c.atmCache.Write(key, blockTime, time.Now(), data)
	if err != nil {
		return nil, err
	}

	return func() ([]byte, error) {
		data, found, err := c.atmCache.Read(key)
		if err != nil {
			return nil, fmt.Errorf("reading disk cache (block %d id %q): %w", blockNum, blockID, err)
		}
		if !found {
			return nil, fmt.Errorf("data for block %d ID %q not found in disk cache", blockNum, blockID)
		}
		return data, nil
	}, nil
}
