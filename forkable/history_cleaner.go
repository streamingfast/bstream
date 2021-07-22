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
	"fmt"

	"github.com/dfuse-io/bstream"
	"go.uber.org/zap"
)

// This gate lets all blocks through once the target blocknum has passed AS IRREVERSIBLE
type HistoryCleaner struct {
	lastSeenHeadBlock bstream.BlockRef
	// libBlockGetter
	targetLibBlockNum uint64
	handler           bstream.Handler
	cliffPassed       bool
	logger            *zap.Logger
}

func NewHistoryCleaner(targetLibBlockNum uint64, h bstream.Handler) *HistoryCleaner {
	hc := &HistoryCleaner{
		targetLibBlockNum: targetLibBlockNum,
		handler:           h,
		logger:            zlog,
	}

	return hc
}

func (h *HistoryCleaner) ProcessBlock(blk *bstream.Block, obj interface{}) error {

	fobj := obj.(*ForkableObject)

	if fobj.Step == StepNew {
		h.lastSeenHeadBlock = blk.AsRef()
	}

	if h.cliffPassed {
		return h.handler.ProcessBlock(blk, obj)
	}

	if fobj.Step != StepIrreversible {
		return nil
	}

	fobj.Step = StepNew // old irreversible blocks as new
	if err := h.handler.ProcessBlock(blk, obj); err != nil {
		return err
	}

	fobj.Step = StepIrreversible // also send it as irreversible
	if err := h.handler.ProcessBlock(blk, obj); err != nil {
		return err
	}

	if blk.Num() == h.targetLibBlockNum {
		h.cliffPassed = true

		fdb := fobj.ForkDB
		blocks := fdb.ReversibleSegment(h.lastSeenHeadBlock)
		// send all blocks as new ---- how to get all values right ? hmmmm
		for _, wrappedBlock := range blocks {
			block := wrappedBlock.Object.(*ForkableBlock)
			blockRef := block.Block.AsRef()
			fo := &ForkableObject{
				Step:        StepNew,
				ForkDB:      fdb,
				lastLIBSent: blk, // last LIB sent == real LIB at that point
				Obj:         block.Obj,
				headBlock:   blockRef,
				block:       blockRef,

				StepIndex: 0,
				StepCount: 1,
				StepBlocks: []*bstream.PreprocessedBlock{
					{
						Block: block.Block,
						Obj:   block.Obj,
					},
				},
			}

			if err := h.handler.ProcessBlock(block.Block, fo); err != nil {
				return fmt.Errorf("process block while catching up [%s] %w", block.Block, err)
			}
		}

	}
	return nil

}
