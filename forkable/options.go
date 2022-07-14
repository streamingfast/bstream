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

package forkable

import (
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

type Option func(f *Forkable)

func WithLogger(logger *zap.Logger) Option {
	return func(f *Forkable) {
		f.logger = logger
	}
}

func WithInclusiveLIB(irreversibleBlock bstream.BlockRef) Option {
	return func(f *Forkable) {
		f.includeInitialLIB = true
		f.forkDB.InitLIB(irreversibleBlock)
	}
}

func WithExclusiveLIB(irreversibleBlock bstream.BlockRef) Option {
	return func(f *Forkable) {
		f.forkDB.InitLIB(irreversibleBlock)
		f.lastLIBSeen = irreversibleBlock
	}
}

// WithFilters choses the steps we want to pass through the sub handler. It defaults to StepsAll upon creation.
func WithFilters(steps bstream.StepType) Option {
	return func(f *Forkable) {
		f.filterSteps = steps
	}
}

func HoldBlocksUntilLIB() Option {
	return func(f *Forkable) {
		f.holdBlocksUntilLIB = true
	}
}

func WithKeptFinalBlocks(count int) Option {
	return func(f *Forkable) {
		f.keptFinalBlocks = count
	}
}

func EnsureBlockFlows(blockRef bstream.BlockRef) Option {
	return func(f *Forkable) {
		f.ensureBlockFlows = blockRef
	}
}

// EnsureAllBlocksTriggerLongestChain will force every block to be
// considered as the longest chain, therefore making it appear as New
// at least once.  The only edge case is if there is a hole between a
// block and LIB when it is received, and it is forked out: in this
// case, that block would never appear.  It is extremely unlikely to
// happen, because incoming blocks should be linkable, and blocks that
// are not forked out will eventually be processed anyway.
func EnsureAllBlocksTriggerLongestChain() Option {
	return func(f *Forkable) {
		f.ensureAllBlocksTriggerLongestChain = true
	}
}
