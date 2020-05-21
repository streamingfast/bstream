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

import "context"

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

type Source interface { //(with a Handler?)
	Run()
	Shutterer // now an interface! WOW!
}

// StartBlockResolver should give you a start block number that will
// guarantee covering all necessary blocks to handle forks before the block
// that you want. This requires chain-specific implementations.
type StartBlockResolver interface {
	Resolve(ctx context.Context, targetBlockNum uint64) (startBlockNum uint64, previousIrreversibleID string, err error)
}

type StartBlockResolverFunc func(context.Context, uint64) (uint64, string, error)

func (s StartBlockResolverFunc) Resolve(ctx context.Context, targetBlockNum uint64) (uint64, string, error) {
	return s(ctx, targetBlockNum)
}

type SourceFactory func(h Handler) Source
type SourceFromRefFactory func(startBlockRef BlockRef, h Handler) Source
type SourceFromNumFactory func(startBlockNum uint64, h Handler) Source
type SourceFromNumFactoryWithErr func(startBlockNum uint64, h Handler) (Source, error)

type StartBlockGetter func() (blockNum uint64)

// Subscriber is a live blocks subscriber implementation
type Subscriber interface {
	Read() (*Block, error)
	StartAtBlockID(ID string) bool
	GetBlockIDInBuffer(blockNum uint64) string
	Start(channelSize int)
	Started() bool
	WaitFor(ID string) <-chan interface{}

	Shutterer
}

type Publisher interface {
	Publish(*Block) (relayed bool)
	Listen() error
}

// Pipeline will process all blocks through the `ProcessBlock()`
// function, unless the pipeline implements also
// `PipelinePreprocessor`, the `obj` will always nil.
//
// If the pipeline was initialized with `irreversibleOnly`, only
// blk.Irreversible blocks will be processed (and post-processed).
// This is when used in conjunction with the irreversible index in
// parallel operations.
type Pipeline interface {
	ProcessBlock(blk *Block, obj interface{}) error
}

type ShortPipelineFunc func(blk *Block) error

func (f ShortPipelineFunc) ProcessBlock(blk *Block, obj interface{}) error {
	return f(blk)
}

type PipelineFunc func(blk *Block, obj interface{}) error

func (f PipelineFunc) ProcessBlock(blk *Block, obj interface{}) error {
	return f(blk, obj)
}

// ParallelPipeline pre-processes the `blk` in parallel. If the
// returned `obj` is non-nil, it is passed to the `ProcessBlock()`
// function. If it is nil, the `ProcessBlock` call is skipped for this
// block.
//
// Even if `ParallelPreprocess` is called in parallel, the calls to
// `ProcessBlock()` are guaranteed to be run linearly in the order
// received from the logs. Note that some blocks can arrive out of
// order when forked. You should be certain of a linear order if the
// Afterburner is configured to feed only irreversible blocks.
type PipelinePreprocessor interface {
	Pipeline
	PreprocessBlock(blk *Block) (obj interface{}, err error)
}

// PipelineStateFlusher implements the FlushState() method, called
// when shutting down the Pipeliner
type PipelineStateFlusher interface {
	FlushState(exitError error) error
}
