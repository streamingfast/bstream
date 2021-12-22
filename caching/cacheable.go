package caching

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/streamingfast/bstream/decoding"
	"sync"
	"time"
)

type Cleanable interface {
	Clean()
}

type CacheableMessage struct {
	engine  *CacheEngine
	key     string
	decoder decoding.Decoder
	recency *timestamp.Timestamp

	memoized     proto.Message
	memoizedLock sync.Mutex
	//fetchFunc      func() (io.ReadCloser, error) //todo: this should be an option

	//originalContentHash []byte // ensures when flushing to cache, that the content was NOT modified if it was a shared message
	//shared              bool
}

func (m *CacheableMessage) SetBytes(input []byte) error {
	return m.engine.setBytes(m, input)
}

func (m *CacheableMessage) SetRecency(timestamp *timestamp.Timestamp) {
	m.recency = timestamp
}

func (m *CacheableMessage) GetBytes() (data []byte, found bool, err error) {
	return m.engine.getBytes(m)
}

func (m *CacheableMessage) GetOwned() (message proto.Message, err error) {
	m.memoizedLock.Lock()
	defer m.memoizedLock.Unlock()
	if m.memoized != nil {
		return m.memoized, nil
	}
	data, found, err := m.GetBytes()
	if err != nil {
		return nil, fmt.Errorf("getting owned message: %w", err)
	}

	if !found {
		panic("todo: call fetch func")
	}

	m.memoized, err = m.decoder.Decode(data)
	m.CleanupIn(10 * time.Second) //todo: need to be configurable
	return m.memoized, nil
}

func (m *CacheableMessage) Clean() {
	m.memoizedLock.Lock()
	defer m.memoizedLock.Unlock()
	m.memoized = nil
}

func (m *CacheableMessage) CleanupIn(duration time.Duration) {
	m.engine.ScheduleCleanup(m, duration)
}

// getShared will return a potentially shared memoized object. WARNING: Do NOT modify the returned object. Use GetOwned if you want to be able to modify the message you receive. Otherwise, do NOT modify the returned `proto.Message`
func (m *CacheableMessage) getShared() (proto.Message, error) {
	panic("getShared not implemented")
	// make sure originalHash is modified

	//m.msgLock.Lock()
	//defer m.msgLock.Unlock()
	//if m.msg == nil {
	//	m.engine.inMemoryMapLock.Lock()
	//	defer m.engine.inMemoryMapLock.Unlock()
	//
	//	cnt, found := m.engine.inMemory[m.key]
	//	if !found {
	//		msg, err := m.bytesToMessage(cnt)
	//		if err != nil {
	//			return nil, err
	//		}
	//
	//		m.msg = msg
	//		return msg, nil
	//	}
	//
	//	// Find on disk
	//	// load in memory? perhaps, depending on caching engine config and rules
	//	// * estimate the chances you'll need to re-read it from disk in the next X minutes, based on what
	//	// decode the proto.Message
	//
	//	// is there a `fetchFunc` set? call that and fetch it from far far away storage
	//	// no fetchFunc, fail
	//}
	//return m.msg, nil
}

//// MarkShared will be used in places like the
//func (m *CacheableMessage) MarkShared() {
//	// because we control the memoized, we can KNOW if we've shared the INSTANCE
//	// reagrdless of the caching, or whenver it was consumed
//	m.shared = true
//}

//func (m *CacheableMessage) SetMessage(msg proto.Message) error {
//	bytes, err := proto.Marshal(msg)
//	if err != nil {
//		return err
//	}
//
//	// lock m.msgLock
//	m.msg = msg
//	return m.engine.setBytes(m.key, bytes)
//}

//// Management of memoization optimizations.
//
//func (m *CacheableMessage) IsOwned() {}
//
//func (m *CacheableMessage) OneLastRead() {}
//
//func (m *CacheableMessage) Free() {
//	m.engine.freeMessage(m.key)
//}
//
