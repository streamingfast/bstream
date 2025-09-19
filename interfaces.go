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

import pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

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
	ProcessSignal(signal *pbbstream.Signal) error
}

type BlockHandlerFunc func(blk *pbbstream.Block, obj any) error
type SignalHandlerFunc func(signal *pbbstream.Signal) error

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

// 3 helpers to create and pipe handlers
func NewHandler(blockHandler BlockHandlerFunc, signalHandler SignalHandlerFunc) Handler {
	return &basicHandler{
		blockHandler:  blockHandler,
		signalHandler: signalHandler,
	}
}

func PassthroughSignalHandler(h Handler) SignalHandlerFunc {
	return func(signal *pbbstream.Signal) error {
		return h.ProcessSignal(signal)
	}
}

func PassthroughBlockHandler(h Handler) BlockHandlerFunc {
	return func(blk *pbbstream.Block, obj any) error {
		return h.ProcessBlock(blk, obj)
	}
}
func DiscardSignal(_ *pbbstream.Signal) error {
	return nil
}

type basicHandler struct {
	blockHandler  BlockHandlerFunc
	signalHandler SignalHandlerFunc
}

func (b *basicHandler) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	return b.blockHandler(blk, obj)
}

func (b *basicHandler) ProcessSignal(signal *pbbstream.Signal) error {
	return b.signalHandler(signal)
}
