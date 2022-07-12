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

type ObjectWrapper interface {
	WrappedObject() interface{}
}

// ForkableSourceFactory allows you to get a stream of fork-aware blocks from either a cursor or a final block
type ForkableSourceFactory interface {
	SourceFromBlockNum(uint64, Handler) Source
	SourceFromCursor(*Cursor, Handler) Source
}

type LowSourceLimitGetter interface {
	LowestBlockNum() uint64
}

type SourceFactory func(h Handler) Source
type SourceFromRefFactory func(startBlockRef BlockRef, h Handler) Source
type SourceFromNumFactory func(startBlockNum uint64, h Handler) Source
type SourceFromNumFactoryWithErr func(startBlockNum uint64, h Handler) (Source, error)

type BlockIndexProviderGetter interface {
	GetIndexProvider() BlockIndexProvider
}

type BlockIndexProvider interface {
	// WithinRange informs you that this block number can be queried on the index
	// Any error accessing the index will return false
	WithinRange(ctx context.Context, blockNum uint64) bool

	// Matches Returns true if that blockNum matches the index
	// When an error is returned, you should assume that this block must go through
	Matches(ctx context.Context, blockNum uint64) (bool, error)

	// NextMatching tries to return the matches from the index.
	// When there is a match under exclusiveUpTo, its number is returned, (num, false, nil)
	// When the end of the index is reached, outsideIndexBoundary is true and the returned blocknum is the first Unindexed block (firstBlockPastIndex, true, nil)
	// When there was an issue reading the index file, the error will be set
	// When there are no matches until exclusiveUpTo, it will be returned (exclusiveUpTo, false, nil)
	NextMatching(ctx context.Context, blockNum, exclusiveUpTo uint64) (num uint64, outsideIndexBoundary bool, err error)
}
