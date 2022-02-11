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
	"go.uber.org/zap"
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
	SetLogger(logger *zap.Logger)
}

type ObjectWrapper interface {
	WrappedObject() interface{}
}

type SourceFactory func(h Handler) Source
type SourceFromRefFactory func(startBlockRef BlockRef, h Handler) Source
type SourceFromNumFactory func(startBlockNum uint64, h Handler) Source
type SourceFromNumFactoryWithErr func(startBlockNum uint64, h Handler) (Source, error)

type BlockIndexProviderGetter interface {
	GetIndexProvider() BlockIndexProvider
}

type BlockIndexProvider interface {
	WithinRange(blockNum uint64) bool
	Matches(blockNum uint64) (bool, error)
	NextMatching(blockNum uint64) (num uint64, outsideIndexBoundary bool, err error) // when outsideIndexBoundary is true, the returned blocknum is the first Unindexed block
}
