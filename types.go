// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
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
	PayloadBuffer  []byte

	// memoized
	memoized interface{}
}

func BlockFromBytes(bytes []byte) (*Block, error) {
	block := new(pbbstream.Block)
	err := proto.Unmarshal(bytes, block)
	if err != nil {
		return nil, fmt.Errorf("unable to read block from bytes: %s", err)
	}

	return BlockFromProto(block)
}

func (b *Block) ToProto() (*pbbstream.Block, error) {
	blockTime, err := ptypes.TimestampProto(b.Time())
	if err != nil {
		return nil, fmt.Errorf("unable to transfrom time value %v to proto time: %s", b.Time(), err)
	}

	return &pbbstream.Block{
		Id:             b.Id,
		Number:         b.Number,
		PreviousId:     b.PreviousId,
		Timestamp:      blockTime,
		LibNum:         b.LibNum,
		PayloadKind:    b.PayloadKind,
		PayloadVersion: b.PayloadVersion,
		PayloadBuffer:  b.PayloadBuffer,
	}, nil
}

func BlockFromProto(b *pbbstream.Block) (*Block, error) {
	blockTime, err := ptypes.Timestamp(b.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("unable to turn google proto Timestamp %q into time.Time: %s", b.Timestamp.String(), err)
	}

	return &Block{
		Id:             b.Id,
		Number:         b.Number,
		PreviousId:     b.PreviousId,
		Timestamp:      blockTime,
		LibNum:         b.LibNum,
		PayloadKind:    b.PayloadKind,
		PayloadVersion: b.PayloadVersion,
		PayloadBuffer:  b.PayloadBuffer,
	}, nil
}

func MustBlockFromProto(b *pbbstream.Block) *Block {
	block, err := BlockFromProto(b)
	if err != nil {
		panic(err)
	}
	return block
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

func (b *Block) Payload() []byte {
	if b == nil {
		return nil
	}

	// Happens when ToNative has been called once
	if b.PayloadBuffer == nil && b.memoized != nil {
		payload, err := proto.Marshal(b.memoized.(proto.Message))
		if err != nil {
			panic(fmt.Errorf("unable to re-encode memoized value to payload: %s", err))
		}

		return payload
	}

	return b.PayloadBuffer
}

func (b *Block) ToNative() interface{} {
	if b.memoized != nil {
		return b.memoized
	}

	if b == nil {
		return nil
	}

	decoder := GetBlockDecoder

	obj, err := decoder.Decode(b)
	if err != nil {
		panic(fmt.Errorf("unable to decode block kind %s version %d (%d payload bytes): %s", b.PayloadKind, b.PayloadVersion, len(b.PayloadBuffer), err))
	}

	b.memoized = obj
	b.PayloadBuffer = nil

	return obj
}

func (b *Block) String() string {
	return blockRefAsAstring(b)
}

// BlockRef represents a reference to a block and is mainly define
// as the pair `<BlockID, BlockNum>`. A `Block` interface should always
// implements the `BlockRef` interface.
//
// The interface enforce also the creation of a `Stringer` object. We expected
// all format to be rendered in the form `#<BlockNum> (<Id>)`. This is to easy
// formatted output when using `zap.Stringer(...)`.
type BlockRef interface {
	ID() string
	Num() uint64
	String() string
}

var BlockRefEmpty = NewBlockRef("", 0)

// BlockRefFromID is a simple wrapper around a string assuming the block number is
// in the first 8 characters of the id as a big endian encoded hexadecimal number
// and the full string represents the ID.
type BlockRefFromID string

func (b BlockRefFromID) ID() string {
	return string(b)
}

func (b BlockRefFromID) Num() uint64 {
	if len(b) < 8 {
		return 0
	}

	bin, err := hex.DecodeString(string(b)[:8])
	if err != nil {
		return 0
	}

	return uint64(binary.BigEndian.Uint32(bin))
}

func (b BlockRefFromID) String() string {
	return blockRefAsAstring(b)
}

// BasicBlockRef assumes the id and num are completely separated
// and represents two independent piece of information. The `ID()`
// in this case is the `id` field and the `Num()` is the `num` field.
type BasicBlockRef struct {
	id  string
	num uint64
}

func NewBlockRef(id string, num uint64) *BasicBlockRef {
	return &BasicBlockRef{id, num}
}

func (b *BasicBlockRef) ID() string {
	return b.id
}

func (b *BasicBlockRef) Num() uint64 {
	return b.num
}

func (b *BasicBlockRef) String() string {
	return blockRefAsAstring(b)
}

// BasicBlockRefFromID is a struct wrapper around `BlockRefFromID`
// but with the `num` field cached extracted from the `BlockRefFromID`.
// This implementation can be used for performanace critical part where
// you don't want to extract the block number over and over again and
// instead having it cached once.
type BasicBlockRefFromID struct {
	id  BlockRefFromID
	num uint64
}

func NewBlockRefFromID(id BlockRefFromID) *BasicBlockRefFromID {
	return &BasicBlockRefFromID{id, id.Num()}
}

func (b *BasicBlockRefFromID) ID() string {
	return string(b.id)
}

func (b *BasicBlockRefFromID) Num() uint64 {
	return b.num
}

func (b *BasicBlockRefFromID) String() string {
	return blockRefAsAstring(b)
}

type gettableBlockNumAndID interface {
	ID() string
	Num() uint64
}

func blockRefAsAstring(source interface{}) string {
	v, ok := source.(gettableBlockNumAndID)
	if source == nil || !ok {
		return "Block <nil>"
	}

	return fmt.Sprintf("#%d (%s)", v.Num(), v.ID())
}
