package caching

import (
	"fmt"
	"github.com/streamingfast/atm"
	"github.com/streamingfast/bstream/decoding"
	"sync"
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
	diskCache       *atm.Cache
	cleanupJobs     map[time.Time][]Cleanable
	cleanupJobsLock sync.Mutex

	cacheableMessageOptions []CacheableMessageOption
}

//todo: disk should be configure through options

func NewCacheEngine(namespace string, diskCache *atm.Cache, cacheableMessageOptions []CacheableMessageOption) *CacheEngine {
	cacheEngine := &CacheEngine{
		namespace:               namespace,
		diskCache:               diskCache,
		cacheableMessageOptions: cacheableMessageOptions,
	}

	cacheEngine.runCleaner()

	return cacheEngine
}

func (e *CacheEngine) NewMessage(key string, decoder decoding.Decoder) *CacheableMessage {
	message := &CacheableMessage{
		engine:  Engine,
		key:     key,
		decoder: decoder,
	}
	message.setOptions(e.cacheableMessageOptions)
	return message
}

func (e *CacheEngine) runCleaner() {
	go func() {
		for {
			time.Sleep(5 * time.Second)
			e.cleanupJobsLock.Lock()

			var toRemove []time.Time
			for when, cleanJobs := range e.cleanupJobs {
				if when.Before(time.Now()) {
					for _, cleanable := range cleanJobs {
						go cleanable.Clean()
					}
					toRemove = append(toRemove, when)
				}
			}

			for _, when := range toRemove {
				delete(e.cleanupJobs, when)
			}

			e.cleanupJobsLock.Unlock()
		}
	}()
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

func (e *CacheEngine) ScheduleCleanup(toClean Cleanable, in time.Duration) {
	e.cleanupJobsLock.Lock()
	defer e.cleanupJobsLock.Unlock()
	when := time.Now().Add(in)

	if jobs, found := e.cleanupJobs[when]; found {
		jobs = append(jobs, toClean)
		return
	}
	jobs := []Cleanable{toClean}
	e.cleanupJobs[when] = jobs
}

type CacheEngineOption func(c *CacheEngine)
