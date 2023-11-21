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

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
)

// BlockRef represents a reference to a block and is mainly define
// as the pair `<BlockID, BlockNum>`. A `Block` interface should always
// implement the `BlockRef` interface.
//
// The interface enforce also the creation of a `Stringer` object. We expected
// all format to be rendered in the form `#<BlockNum> (<Id>)`. This is to easy
// formatted output when using `zap.Stringer(...)`.
type BlockRef interface {
	ID() string
	Num() uint64
	String() string
}

type Pretty interface {
	Pretty() string
}

var BlockRefEmpty BlockRef = &emptyBlockRef{}

type emptyBlockRef struct{}

func (e *emptyBlockRef) Num() uint64    { return 0 }
func (e *emptyBlockRef) ID() string     { return "" }
func (e *emptyBlockRef) String() string { return "Block <empty>" }

// BasicBlockRef assumes the id and num are completely separated
// and represents two independent piece of information. The `ID()`
// in this case is the `id` field and the `Num()` is the `num` field.
type BasicBlockRef struct {
	id  string
	num uint64
}

func NewBlockRef(id string, num uint64) BasicBlockRef {
	return BasicBlockRef{id, num}
}

// NewBlockRefFromID is a convenience method when the string is assumed to have
// the block number in the first 8 characters of the id as a big endian encoded
// hexadecimal number and the full string represents the ID.
func NewBlockRefFromID(id string) BasicBlockRef {
	if len(id) < 8 {
		return BasicBlockRef{id, 0}
	}

	bin, err := hex.DecodeString(string(id)[:8])
	if err != nil {
		return BasicBlockRef{id, 0}
	}

	return BasicBlockRef{id, uint64(binary.BigEndian.Uint32(bin))}
}

func (b BasicBlockRef) ID() string {
	return b.id
}

func (b BasicBlockRef) Num() uint64 {
	return b.num
}

func (b BasicBlockRef) String() string {
	return blockRefAsAstring(b)
}

func IsEmpty(ref BlockRef) bool {
	if ref == nil {
		return true
	}

	return ref.Num() == 0 && ref.ID() == ""
}

func EqualsBlockRefs(left, right BlockRef) bool {
	if left == right {
		return true
	}

	if left == nil || right == nil {
		return false
	}

	return left.Num() == right.Num() && left.ID() == right.ID()
}

type gettableBlockNumAndID interface {
	ID() string
	Num() uint64
}

func blockRefAsAstring(source gettableBlockNumAndID) string {
	if source == nil {
		return "Block <nil>"
	}

	return fmt.Sprintf("#%d (%s)", source.Num(), source.ID())
}

type BlockWithObj struct {
	Block *pbbstream.Block
	Obj   interface{}
}

type wrappedObject struct {
	obj                interface{}
	cursor             *Cursor
	reorgJunctionBlock BlockRef
}

func (w *wrappedObject) FinalBlockHeight() uint64 {
	if w.cursor.LIB == nil {
		return 0
	}
	return w.cursor.LIB.Num()
}

func (w *wrappedObject) ReorgJunctionBlock() BlockRef {
	return w.reorgJunctionBlock
}

func (w *wrappedObject) Step() StepType {
	return w.cursor.Step
}

func (w *wrappedObject) WrappedObject() interface{} {
	return w.obj
}

func (w *wrappedObject) Cursor() *Cursor {
	return w.cursor
}
