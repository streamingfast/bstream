package bstream

import (
	"fmt"
	"time"

	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Block reprensents a block envelope abstraction across all
// StreamingFast systems and for now is wide enough to accomodate a
// varieties of implementation. It's the actual stucture that flows
// all around `bstream`.
type Block struct {
	Id         string
	Number     uint64
	PreviousId string
	Timestamp  time.Time
	LibNum     uint64 // This is non-deterministic on some chains, but should move only when a node confirmed a block number has passed the threshold of finality/mucho-confirmation/irreversibility.

	// These are segregated from the anypb.Any to allow for on-disk caching,
	// freeing up lots of memory.
	PayloadType string       // corresponds to the `TypeUrl` of the anypb.Any
	GetPayload  GetBytesFunc // corresponds to the `Value` of anypb.Any
}

func NewBlockFromBytes(bytes []byte, cacher CacheBytesFunc) (*Block, error) {
	block := new(pbbstream.Block)
	err := proto.Unmarshal(bytes, block)
	if err != nil {
		return nil, fmt.Errorf("unable to read sf.bstream.v1.Block from bytes: %w", err)
	}

	return NewBlockFromProto(block, cacher)
}

func NewBlockFromProto(b *pbbstream.Block, cacher CacheBytesFunc) (*Block, error) {
	blockTime := b.Timestamp.AsTime()

	// PayloadBuffer & PayloadKind & PayloadVersion
	// Payload (any)

	payload := b.PayloadBuffer
	payloadType := b.Payload.TypeUrl
	switch b.PayloadKind {
	case pbbstream.Protocol_UNKNOWN:
		payload = b.Payload.Value
	case pbbstream.Protocol_EOS:
		payloadType = "sf.eosio.type.v1.Block"
	case pbbstream.Protocol_ETH:
		payloadType = "sf.ethereum.type.v1.Block"
	case pbbstream.Protocol_SOLANA:
		payloadType = "sf.solana.type.v1.Block"
	case pbbstream.Protocol_NEAR:
		payloadType = "sf.near.type.v1.Block"
	case pbbstream.Protocol_COSMOS:
		payloadType = "sf.cosmos.type.v1.Block"
	}

	block := &Block{
		Id:          b.Id,
		Number:      b.Number,
		PreviousId:  b.PreviousId,
		Timestamp:   blockTime,
		LibNum:      b.LibNum,
		PayloadType: payloadType,
	}
	if cacher == nil {
		block.GetPayload = func() ([]byte, error) { return payload, nil }
		return block, nil
	}
	var err error
	block.GetPayload, err = cacher(block, payload)
	return block, err
}

func MustNewBlockFromProto(b *pbbstream.Block, cacher CacheBytesFunc) *Block {
	block, err := NewBlockFromProto(b, cacher)
	if err != nil {
		panic(err)
	}
	return block
}

func (b *Block) ToProto() (*pbbstream.Block, error) {
	blockTime := timestamppb.New(b.Time())

	anyPayload, err := b.PayloadToAny()
	if err != nil {
		return nil, fmt.Errorf("retrieving payload for block: %d %s: %w", b.Num(), b.ID(), err)
	}

	return &pbbstream.Block{
		Id:         b.Id,
		Number:     b.Number,
		PreviousId: b.PreviousId,
		Timestamp:  blockTime,
		LibNum:     b.LibNum,
		Payload:    anyPayload,
	}, nil
}

func (b *Block) PayloadToAny() (*anypb.Any, error) {
	payload, err := b.GetPayload()
	if err != nil {
		return nil, fmt.Errorf("retrieving payload for block: %d %s: %w", b.Num(), b.ID(), err)
	}

	return &anypb.Any{
		TypeUrl: "type.googleapis.com/" + b.PayloadType,
		Value:   payload,
	}, nil
}

func (b *Block) UnmarshalPayload() (proto.Message, error) {
	anyPayload, err := b.PayloadToAny()
	if err != nil {
		return nil, fmt.Errorf("payload to any: %w", err)
	}
	return anyPayload.UnmarshalNew()
}

// TODO: unsure why we'd have these fields?
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

func (b *Block) PreviousRef() BlockRef {
	if b == nil || b.Number == 0 || b.PreviousId == "" {
		return BlockRefEmpty
	}
	return NewBlockRef(b.PreviousId, b.Number-1)
}

func (b *Block) String() string {
	return blockRefAsAstring(b)
}
