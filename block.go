package bstream

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	pbany "github.com/golang/protobuf/ptypes/any"
	"github.com/streamingfast/bstream/caching"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"time"
)

// Block reprensents a block abstraction across all dfuse systems
// and for now is wide enough to accomodate a varieties of implementation. It's
// the actual stucture that flows all around `bstream`.
type Block struct {
	Id         string
	Number     uint64
	PreviousId string
	Timestamp  time.Time
	LibNum     uint64

	PayloadKind    pbbstream.Protocol
	PayloadVersion int32

	Native *caching.CacheableMessage
}

func NewBlockFromBytes(bytes []byte) (*Block, error) {
	block := new(pbbstream.Block)
	err := proto.Unmarshal(bytes, block)
	if err != nil {
		return nil, fmt.Errorf("unable to read block from bytes: %w", err)
	}

	return NewBlockFromProto(block)
}

func NewBlockFromProto(b *pbbstream.Block) (*Block, error) {
	blockTime, err := ptypes.Timestamp(b.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("unable to turn google proto Timestamp %q into time.Time: %w", b.Timestamp.String(), err)
	}

	block := &Block{
		Id:             b.Id,
		Number:         b.Number,
		PreviousId:     b.PreviousId,
		Timestamp:      blockTime,
		LibNum:         b.LibNum,
		PayloadKind:    b.PayloadKind,
		PayloadVersion: b.PayloadVersion,
		Native:         caching.Engine.NewMessage(fmt.Sprintf("block-%s", b.Id), GetBlockDecoder),
	}

	err = block.Native.SetBytes(b.PayloadBuffer)
	if err != nil {
		return nil, fmt.Errorf("setting native bytes: %w", err)
	}
	block.Native.SetRecency(b.Timestamp)

	return block, nil
}

func MustNewBlockFromProto(b *pbbstream.Block) *Block {
	block, err := NewBlockFromProto(b)
	if err != nil {
		panic(err)
	}
	return block
}

func (b *Block) ToAny(decoded bool, interceptor func(blockMessage proto.Message) proto.Message) (*pbany.Any, error) {
	if decoded {
		blk, err := b.Native.GetOwned()
		if err != nil {
			return nil, fmt.Errorf("getting owned message: %w", err)
		}

		if interceptor != nil {
			blk = interceptor(blk)
		}

		pbBlk, ok := blk.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("block interface is not of expected type proto.Message, got %T", blk)
		}

		return ptypes.MarshalAny(pbBlk)
	}

	blk, err := b.ToProto()
	if err != nil {
		return nil, fmt.Errorf("to proto: %w", err)
	}

	return ptypes.MarshalAny(blk)
}

func (b *Block) ToProto() (*pbbstream.Block, error) {
	blockTime, err := ptypes.TimestampProto(b.Time())
	if err != nil {
		return nil, fmt.Errorf("unable to transfrom time value %v to proto time: %w", b.Time(), err)
	}

	// Memoized, serialize it, and stuff it in PayloadBuffer
	//todo: call cache here ...
	payload, found, err := b.Native.GetBytes()
	if err != nil {
		return nil, fmt.Errorf("retrieving payload for block: %d %s: %w", b.Num(), b.ID(), err)
	}

	if !found {
		return nil, fmt.Errorf("payload not found for block: %d %s: %w", b.Num(), b.ID(), err)
	}

	return &pbbstream.Block{
		Id:             b.Id,
		Number:         b.Number,
		PreviousId:     b.PreviousId,
		Timestamp:      blockTime,
		LibNum:         b.LibNum,
		PayloadKind:    b.PayloadKind,
		PayloadVersion: b.PayloadVersion,
		PayloadBuffer:  payload,
	}, nil
}

func (b *Block) ID() string {
	if b == nil {
		return ""
	}

	return b.Id
}

func (b *Block) Num() uint64 {
	if b == nil {
		return 0
	}

	return b.Number
}

func (b *Block) PreviousID() string {
	if b == nil {
		return ""
	}

	return b.PreviousId
}

func (b *Block) Time() time.Time {
	if b == nil {
		return time.Time{}
	}

	return b.Timestamp
}

func (b *Block) LIBNum() uint64 {
	if b == nil {
		return 0
	}

	return b.LibNum
}

func (b *Block) Kind() pbbstream.Protocol {
	if b == nil {
		return pbbstream.Protocol_UNKNOWN
	}

	return b.PayloadKind
}

func (b *Block) Version() int32 {
	if b == nil {
		return -1
	}

	return b.PayloadVersion
}

func (b *Block) AsRef() BlockRef {
	if b == nil {
		return BlockRefEmpty
	}

	return NewBlockRef(b.Id, b.Number)
}

func (b *Block) PreviousRef() BlockRef {
	if b == nil || b.Number == 0 || b.PreviousId == "" {
		return BlockRefEmpty
	}

	return NewBlockRef(b.PreviousId, b.Number-1)
}

// Deprecated: ToNative is deprecated, it is replaced by ToProtocol significantly more intuitive naming.
func (b *Block) ToNative() interface{} {
	return b.ToProtocol()
}

// Deprecated: ToNative is deprecated, b.Native.Get()
func (b *Block) ToProtocol() interface{} {
	if b == nil {
		return nil
	}

	obj, err := b.Native.GetOwned()
	if err != nil {
		panic(fmt.Errorf("unable to decode block kind %s version %d : %w", b.PayloadKind, b.PayloadVersion, err))
	}

	return obj
}

func (b *Block) String() string {
	return blockRefAsAstring(b)
}
