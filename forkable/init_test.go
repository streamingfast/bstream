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
	"testing"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	logging.InstantiateLoggers()
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

func tinyBlk(id string) bstream.BlockRef {
	return bstream.TestBlock(id, "").AsRef()
}

func bTestBlock(id, previousID string) *pbbstream.Block {
	return bstream.TestBlock(id, previousID)
}

func tb(id, previousID string, newLIB uint64) *pbbstream.Block {
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

func (p *testForkableSink) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
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

func assertBlockAndCursors(t *testing.T, expected, actual []*blockAndCursor) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for idx, expect := range expected {
		assertBlockAndCursor(t, expect, actual[idx])
	}
}

func assertBlockAndCursor(t *testing.T, expected, actual *blockAndCursor) {
	t.Helper()

	bstream.AssertCursorEqual(t, expected.cursor, actual.cursor)
	bstream.AssertProtoEqual(t, expected.block, actual.block)
}

func assertExpectedBlocks(t *testing.T, expected, actual []expectedBlock) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for idx, expect := range expected {
		assertExpectedBlock(t, expect, actual[idx])
	}
}

func assertExpectedBlock(t *testing.T, expected, actual expectedBlock) {
	t.Helper()

	assert.Equal(t, expected.step, actual.step)
	assert.Equal(t, expected.cursorLibNum, actual.cursorLibNum)
	bstream.AssertProtoEqual(t, expected.block, actual.block)
}
