package caching

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/streamingfast/bstream/decoding"
)

type CacheableMessage struct {
	engine  *CacheEngine
	key     string
	decoder decoding.Decoder
	recency *timestamp.Timestamp

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
	data, found, err := m.GetBytes()
	if err != nil {
		return nil, fmt.Errorf("getting owned message: %w", err)
	}

	if !found {
		panic("todo: call fetch func")
	}

	message, err = m.decoder.Decode(data)
	return
	// DECODE from BYTES, and do NOT set it on `m.msg`, yet RETURN it.
	// so it is completely detachd from this CacheableMessage instance.
	// You can wrap it with a new key somewhere else if you want.

	// If `!shared` (you OWN it, you're in a Handler that is known to be solely yours)
	// well you can simply return the `proto.Message`.
	//
	// clones if it was shared
	// don't clone if it was already owned
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
//func (m *CacheableMessage) FreeIn(time.Duration) {
//
//}
