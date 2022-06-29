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
	"os"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

func init() {
	if os.Getenv("DEBUG") != "" || os.Getenv("TRACE") == "true" {
		logger, _ := zap.NewDevelopment()
		logging.Override(logger)
	}
}

func bRefInSegment(num uint64, segment string) bstream.BlockRef {
	return bstream.NewBlockRefFromID(fmt.Sprintf("%08x%s", num, segment))
}

func prevRef(ref bstream.BlockRef) bstream.BlockRef {
	return bRefInSegment(ref.Num()-1, ref.ID()[8:])
}

func bRef(id string) bstream.BlockRef {
	return bstream.NewBlockRef(id, blocknum(id))
}

func tinyBlk(id string) *bstream.Block {
	return bstream.TestBlock(id, "")
}

func bTestBlock(id, previousID string) *bstream.Block {
	return bstream.TestBlock(id, previousID)
}

func tb(id, previousID string, newLIB uint64) *bstream.Block {
	if newLIB == 0 {
		return bstream.TestBlock(id, previousID)
	}
	return bstream.TestBlockWithLIBNum(id, previousID, newLIB)
}

type testForkableSink struct {
	results []*ForkableObject
	undoErr error
	newErr  error
}

func newTestForkableSink(undoErr, newErr error) *testForkableSink {
	return &testForkableSink{
		results: []*ForkableObject{},
		undoErr: undoErr,
		newErr:  newErr,
	}
}

func (p *testForkableSink) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	fao := obj.(*ForkableObject)

	if fao.step == bstream.StepUndo && p.undoErr != nil {
		return p.undoErr
	}

	if fao.step == bstream.StepNew && p.newErr != nil {
		return p.newErr
	}

	p.results = append(p.results, fao)
	return nil
}

func fdbLinkedWithoutLIB(kv ...string) *ForkDB {
	return fdbLinked("", kv...)
}

func fdbLinked(lib string, kv ...string) *ForkDB {
	fDB := NewForkDB()
	if lib != "" {
		fDB.InitLIB(bRef(lib))
	}

	for i := 0; i < len(kv); i += 3 {
		blockID := kv[i]
		previousID := kv[i+1]
		blk := bTestBlock(blockID, previousID)
		fDB.AddLink(bRef(blockID), previousID, &ForkableBlock{Block: blk, Obj: kv[i+2]})
	}

	return fDB
}
