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
	"context"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
)

type Shutterer interface {
	Shutdown(error)
	Terminating() <-chan struct{}
	IsTerminating() bool
	Terminated() <-chan struct{}
	IsTerminated() bool
	OnTerminating(f func(error))
	OnTerminated(f func(error))
	Err() error
}

type Handler interface {
	ProcessBlock(blk *pbbstream.Block, obj any) error
}

type HandlerFunc func(blk *pbbstream.Block, obj any) error

func (h HandlerFunc) ProcessBlock(blk *pbbstream.Block, obj any) error {
	return h(blk, obj)
}

type PreprocessFunc func(blk *pbbstream.Block) (any, error)

type Source interface {
	Shutterer
	Run()
}

type ForkableObject interface {
	Cursorable
	Stepable
	ObjectWrapper
}

type Cursorable interface {
	Cursor() *Cursor
}

type Stepable interface {
	Step() StepType
	FinalBlockHeight() uint64
	ReorgJunctionBlock() BlockRef
}

type Liveable interface {
	IsLiveBlock() bool // Returns true if the object was sent by a live stream, without any buffer
	SetLiveBlock(bool) // Used to set the live status of the object, let's say you hit a full buffer and want to switch it to false
}

type ObjectWrapper interface {
	WrappedObject() any
}

// ForkableSourceFactory allows you to get a stream of fork-aware blocks from either a cursor or a final block
type ForkableSourceFactory interface {
	SourceFromBlockNum(uint64, Handler) Source // irreversible
	SourceFromCursor(*Cursor, Handler) Source
	SourceThroughCursor(uint64, *Cursor, Handler) Source
}

type LowSourceLimitGetter interface {
	LowestBlockNum() uint64
}

// LiveBlockKnower is implemented by a live source factory that can say which block range
// it holds and whether a block ID is one of the blocks in it. Over that range the live
// source is authoritative: a block ID it does not know is on no chain it ever saw.
type LiveBlockKnower interface {
	LowestBlockNum() uint64
	HeadNum() uint64
	GetBlockByHash(id string) *pbbstream.Block
}

// ForkedBlockKnower is implemented by a file source factory that can say whether the
// forked-blocks store holds a block, which is the other place a cursor sitting on a fork
// can be resolved from once the live source no longer has it.
type ForkedBlockKnower interface {
	HasForkedBlock(ctx context.Context, idSuffix string, blockNum uint64) (bool, error)
}

type SourceFactory func(h Handler) Source
type SourceFromRefFactory func(startBlockRef BlockRef, h Handler) Source
type SourceFromNumFactory func(startBlockNum uint64, h Handler) Source
type SourceFromNumFactoryWithSkipFunc func(startBlockNum uint64, h Handler, skipFunc func(idSuffix string) bool) Source

type BlockIndexProviderGetter interface {
	GetIndexProvider() BlockIndexProvider
}

type BlockIndexProvider interface {
	BlocksInRange(baseBlockNum, bundleSize uint64) (out []uint64, err error)
}
