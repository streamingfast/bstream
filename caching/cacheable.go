package caching

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/streamingfast/bstream/decoding"
	"io"
	"sync"
	"time"
)

type Cleanable interface {
	Clean()
}

type FetchFunc func(namespace string, key string) (io.ReadCloser, error)

type CacheableMessage struct {
	engine  *CacheEngine
	key     string
	decoder decoding.Decoder
	recency *timestamp.Timestamp

	memoizedPayload []byte
	memoizedMessage proto.Message
	lock            sync.Mutex
	fetchFunc       FetchFunc

	//originalContentHash []byte // ensures when flushing to cache, that the content was NOT modified if it was a shared message
	//shared              bool
	memoizedDuration time.Duration
}

func (m *CacheableMessage) setOptions(options []CacheableMessageOption) *CacheableMessage {
	for _, option := range options {
		option(m)
	}
	return m
}

func (m *CacheableMessage) SetBytes(input []byte) error {
	return m.engine.setBytes(m, input)
}

func (m *CacheableMessage) SetRecency(timestamp *timestamp.Timestamp) {
	m.recency = timestamp
}

func (m *CacheableMessage) GetBytes() (data []byte, found bool, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	//todo: maybe optimize by checking if memoizedMessage is not nil and serialize it

	return m.getBytes()
}

//Always called from inside a locked function
func (m *CacheableMessage) getBytes() (data []byte, found bool, err error) {
	if m.memoizedPayload != nil {
		return m.memoizedPayload, true, nil
	}

	data, found, err = m.engine.getBytes(m)

	if m.memoizedDuration > 0 {
		if found && err == nil {
			m.memoizedPayload = data
			m.CleanupIn(m.memoizedDuration)
		}
	}

	return
}

func (m *CacheableMessage) GetOwned() (message proto.Message, err error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.memoizedMessage != nil {
		return m.memoizedMessage, nil
	}

	data, found, err := m.getBytes()
	if err != nil {
		return nil, fmt.Errorf("getting owned message: %w", err)
	}

	if !found {
		panic("todo: call fetch func")
	}
	decodedMessage, err := m.decoder.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("decoding data to message: %w", err)
	}
	if m.memoizedDuration > 0 {
		m.memoizedPayload = nil //not need if we got a memoized
		m.memoizedMessage = decodedMessage
		m.CleanupIn(m.memoizedDuration)
	}

	return decodedMessage, nil
}

func (m *CacheableMessage) Clean() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.memoizedMessage = nil
	m.memoizedPayload = nil
}

func (m *CacheableMessage) CleanupIn(duration time.Duration) {
	m.engine.ScheduleCleanup(m, duration)
}

type CacheableMessageOption func(c *CacheableMessage)

func WithMemoizedDuration(duration time.Duration) CacheableMessageOption {
	return func(c *CacheableMessage) {
		c.memoizedDuration = duration
	}
}

func WithFetchFunc(fetchFunc FetchFunc) CacheableMessageOption {
	return func(c *CacheableMessage) {
		c.fetchFunc = fetchFunc
	}
}

// getShared will return a potentially shared memoizedMessage object. WARNING: Do NOT modify the returned object. Use GetOwned if you want to be able to modify the message you receive. Otherwise, do NOT modify the returned `proto.Message`
//func (m *CacheableMessage) getShared() (proto.Message, error) {
//	panic("getShared not implemented")
//	// make sure originalHash is modified
//
//	//m.msgLock.Lock()
//	//defer m.msgLock.Unlock()
//	//if m.msg == nil {
//	//	m.engine.inMemoryMapLock.Lock()
//	//	defer m.engine.inMemoryMapLock.Unlock()
//	//
//	//	cnt, found := m.engine.inMemory[m.key]
//	//	if !found {
//	//		msg, err := m.bytesToMessage(cnt)
//	//		if err != nil {
//	//			return nil, err
//	//		}
//	//
//	//		m.msg = msg
//	//		return msg, nil
//	//	}
//	//
//	//	// Find on disk
//	//	// load in memory? perhaps, depending on caching engine config and rules
//	//	// * estimate the chances you'll need to re-read it from disk in the next X minutes, based on what
//	//	// decode the proto.Message
//	//
//	//	// is there a `fetchFunc` set? call that and fetch it from far far away storage
//	//	// no fetchFunc, fail
//	//}
//	//return m.msg, nil
//}

//// MarkShared will be used in places like the
//func (m *CacheableMessage) MarkShared() {
//	// because we control the memoizedMessage, we can KNOW if we've shared the INSTANCE
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
