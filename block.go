package bstream

import (
	"fmt"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"reflect"
	"time"

	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	proto "google.golang.org/protobuf/proto"
)

var GetMemoizeMaxAge time.Duration

// Block reprensents a block abstraction across all dfuse systems
// and for now is wide enough to accomodate a varieties of implementation. It's
// the actual stucture that flows all around `bstream`.
type Block struct {
	Id          string
	Number      uint64
	PreviousId  string
	PreviousNum uint64
	Timestamp   time.Time
	LibNum      uint64

	Payload *anypb.Any
	cloned  bool
}

func NewBlockFromBytes(bytes []byte) (*Block, error) {
	block := new(pbbstream.Block)
	if err := proto.Unmarshal(bytes, block); err != nil {
		return nil, fmt.Errorf("unable to read block from bytes: %w", err)
	}

	return NewBlockFromProto(block)
}

func NewBlockFromProto(b *pbbstream.Block) (*Block, error) {
	if err := b.Timestamp.CheckValid(); err != nil {
		return nil, fmt.Errorf("unable to turn google proto Timestamp %q into time.Time: %w", b.Timestamp.String(), err)
	}

	if b.Payload == nil {
		switch b.PayloadKind {
		case pbbstream.Protocol_EOS:
			b.Payload.TypeUrl = "sf.antelope.type.v1.Block"
		case pbbstream.Protocol_ETH:
			b.Payload.TypeUrl = "sf.ethereum.type.v2.Block"
		case pbbstream.Protocol_COSMOS:
			b.Payload.TypeUrl = "sf.cosmos.type.v1.Block"
		case pbbstream.Protocol_SOLANA:
			return nil, fmt.Errorf("old block format from Solana protocol not supported, migrate your blocks")
		case pbbstream.Protocol_NEAR:
			return nil, fmt.Errorf("old block format from NEAR protocol not supported, migrate your blocks")
		}
		b.Payload.Value = b.PayloadBuffer
		b.Payload.TypeUrl = "type.googleapis.com/" + b.Payload.TypeUrl
		if GetProtocolFirstStreamableBlock != b.Number {
			b.PreviousNum = b.Number - 1
		}
	}

	return &Block{
		Id:          b.Id,
		Number:      b.Number,
		PreviousId:  b.PreviousId,
		PreviousNum: b.PreviousNum,
		Timestamp:   b.Timestamp.AsTime(),
		LibNum:      b.LibNum,
		Payload:     b.Payload,
	}, nil
}

func MustNewBlockFromProto(b *pbbstream.Block) *Block {
	block, err := NewBlockFromProto(b)
	if err != nil {
		panic(err)
	}
	return block
}

func (b *Block) Clone() *Block {
	return &Block{
		Id:         b.Id,
		Number:     b.Number,
		PreviousId: b.PreviousId,
		Timestamp:  b.Timestamp,
		LibNum:     b.LibNum,
		//PayloadKind:    b.PayloadKind,
		//PayloadVersion: b.PayloadVersion,
		Payload: b.Payload,
		cloned:  true,
	}
}

func (b *Block) ToProto() (*pbbstream.Block, error) {
	//payload, err := b.Payload.Get()
	//if err != nil {
	//	return nil, fmt.Errorf("retrieving payload for block: %d %s: %w", b.Num(), b.ID(), err)
	//}

	return &pbbstream.Block{
		Id:         b.Id,
		Number:     b.Number,
		PreviousId: b.PreviousId,
		Timestamp:  timestamppb.New(b.Time()),
		LibNum:     b.LibNum,
		Payload:    b.Payload,
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

func (b *Block) AsRef() BlockRef {
	if b == nil {
		return BlockRefEmpty
	}

	return NewBlockRef(b.Id, b.Number)
}

func ToProtocol[B proto.Message](blk *Block) B {
	var b B
	value := reflect.New(reflect.TypeOf(b).Elem()).Interface().(B)
	if err := blk.Payload.UnmarshalTo(value); err != nil {
		panic(fmt.Errorf("unable to unmarshal block %s payload (kind: %s): %w", blk.AsRef(), blk.Payload.TypeUrl, err))
	}
	return value
}

func (b *Block) String() string {
	return blockRefAsAstring(b)
}
