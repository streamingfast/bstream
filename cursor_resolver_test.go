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
	"errors"
	"testing"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
)

type blockWithStep struct {
	blk  BlockRef
	step StepType
}

var errDone = errors.New("test done")

func TestCursorResolver(t *testing.T) {
	merged := dstore.NewMockStore(nil)
	merged.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "1a", 1,
		3, "3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1,
		4, "4aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 2,
	))

	forked := dstore.NewMockStore(nil)
	forked.SetFile(BlockFileName(&Block{
		Id:         "3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Number:     3,
		PreviousId: "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LibNum:     1,
	}), testBlocks(3, "3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
	)

	expected := []blockWithStep{
		{
			blk:  &BasicBlockRef{id: "3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", num: 3},
			step: StepUndo,
		},
		{
			blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", num: 3},
			step: StepNewIrreversible,
		},
		{
			blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", num: 4},
			step: StepNewIrreversible,
		},
	}

	i := 0
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		assert.Equal(t, blk.String(), expected[i].blk.String())
		assert.Equal(t, obj.(Stepable).Step().String(), expected[i].step.String())
		i++
		if i == len(expected) {
			return errDone
		}
		return nil
	})

	fs := NewFileSourceFromCursor(merged, forked, &Cursor{
		Step:      StepNew,
		Block:     NewBlockRef("3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3),
		HeadBlock: NewBlockRef("3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3),
		LIB:       NewBlockRef("1a", 2),
	}, handler, zlog)
	testDone := make(chan struct{})
	go func() {
		fs.Run()
		close(testDone)
	}()
	select {
	case <-testDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}
	assert.ErrorIs(t, fs.Err(), errDone)
}

func TestCursorResolverWithHoles(t *testing.T) {
	merged := dstore.NewMockStore(nil)
	merged.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "1a", 1,
		4, "4aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 2,
	))

	forked := dstore.NewMockStore(nil)
	forked.SetFile(BlockFileName(&Block{
		Id:         "3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Number:     3,
		PreviousId: "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		LibNum:     1,
	}), testBlocks(3, "3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
	)

	expected := []blockWithStep{
		{
			blk:  &BasicBlockRef{id: "3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", num: 3},
			step: StepUndo,
		},
		{
			blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", num: 4},
			step: StepNewIrreversible,
		},
	}

	i := 0
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		assert.Equal(t, blk.String(), expected[i].blk.String())
		assert.Equal(t, obj.(Stepable).Step().String(), expected[i].step.String())
		i++
		if i == len(expected) {
			return errDone
		}
		return nil
	})

	fs := NewFileSourceFromCursor(merged, forked, &Cursor{
		Step:      StepNew,
		Block:     NewBlockRef("3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3),
		HeadBlock: NewBlockRef("3bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 3),
		LIB:       NewBlockRef("1a", 2),
	}, handler, zlog)
	testDone := make(chan struct{})
	go func() {
		fs.Run()
		close(testDone)
	}()
	select {
	case <-testDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}
	assert.ErrorIs(t, fs.Err(), errDone)
}
