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
	ProcessBlock(blk *Block, obj interface{}) error
}

type HandlerFunc func(blk *Block, obj interface{}) error

func (h HandlerFunc) ProcessBlock(blk *Block, obj interface{}) error {
	return h(blk, obj)
}

type PreprocessFunc func(blk *Block) (interface{}, error)

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
}

type ObjectWrapper interface {
	WrappedObject() interface{}
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
