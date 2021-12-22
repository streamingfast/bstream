package caching

import (
	"fmt"
	"github.com/streamingfast/atm"
	"github.com/streamingfast/bstream/decoding"
	"time"
)

type CacheEngine struct {
	namespace string
	//cachePath         string
	//dstorePath        string
	//inMemory          map[string][]byte
	//inMemoryMapLock   sync.Mutex
	//inMemoryLRUStatus map[string]time.Time
	//fileStorage       map[string]string // file path where the bytes content are stored
	//diskLRUStatus     map[string]time.Time
	diskCache *atm.Cache
}

func NewCacheEngine(namespace string, diskCache *atm.Cache) *CacheEngine {
	return &CacheEngine{
		namespace: namespace,
		diskCache: diskCache,
	}
}

func (e *CacheEngine) NewMessage(key string, decoder decoding.Decoder) *CacheableMessage {
	return &CacheableMessage{
		engine:  Engine,
		key:     key,
		decoder: decoder,
	}
}

func (e *CacheEngine) SetupCleanupper() {
	//// launched in go routine
	//for {
	//	time.Sleep(10 * time.Second)
	//	// LOCK inMemory here
	//	for key, value := range e.inMemory {
	//		if memorizedLRU[key].After(time.Now()) {
	//
	//		}
	//		if e.diskLRUStatus[key].After(time.Now()) {
	//			// purge from inMemory, purge from lruStatus
	//			//
	//		}
	//	}
	//}
}

func (e *CacheEngine) namespacedKey(cacheableMessage *CacheableMessage) string {
	return fmt.Sprintf("%s-%s", e.namespace, cacheableMessage.key)
}

func (e *CacheEngine) setBytes(cacheableMessage *CacheableMessage, input []byte) (err error) {
	//todo: handle the case where the diskCache is not set ...
	namespacedKey := e.namespacedKey(cacheableMessage)
	_, err = e.diskCache.Write(namespacedKey, cacheableMessage.recency.AsTime(), time.Now(), input)
	// first set in memory? according to current config policy?
	// write to disk and whipe out directly?
	// lock appropriate stuff?
	// do we have a fetchFunc for that key? if not, never jart it from local disk
	// keep the bytes size in a sort of CachedItem, for optimization
	return
}

func (e *CacheEngine) getBytes(cacheableMessage *CacheableMessage) (data []byte, found bool, err error) {
	namespacedKey := e.namespacedKey(cacheableMessage)
	//todo: handle the case where the diskCache is not set ...
	data, found, err = e.diskCache.Read(namespacedKey)
	return
}
