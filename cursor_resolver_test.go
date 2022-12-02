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

	cases := []struct {
		name         string
		blocks       []byte
		cursor       Cursor
		expected     []blockWithStep
		forkedBlocks map[string][]byte
	}{
		{
			"new on forked block",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepNew,
				Block:     NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				HeadBlock: NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 2),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "3bbbbbbbbbbbbbbb", num: 3},
					step: StepUndo,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			map[string][]byte{
				BlockFileName(&Block{Id: "3bbbbbbbbbbbbbbb", Number: 3, PreviousId: "2aaaaaaaaaaaaaaa", LibNum: 1}): testBlocks(3, "3bbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaa", 1),
			},
		},
		{
			"new on good block",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepNew,
				Block:     NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				HeadBlock: NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 1),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			map[string][]byte{
				BlockFileName(&Block{Id: "3bbbbbbbbbbbbbbb", Number: 3, PreviousId: "2aaaaaaaaaaaaaaa", LibNum: 1}): testBlocks(3, "3bbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaa", 1),
			},
		},

		{
			"deep reorg",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "0aaaaaaaaaaaaaaa", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepNew,
				Block:     NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				HeadBlock: NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 1),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "3bbbbbbbbbbbbbbb", num: 3},
					step: StepUndo,
				},
				{
					blk:  &BasicBlockRef{id: "2bbbbbbbbbbbbbbb", num: 2},
					step: StepUndo,
				},
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			map[string][]byte{
				BlockFileName(&Block{Id: "3bbbbbbbbbbbbbbb", Number: 3, PreviousId: "2bbbbbbbbbbbbbbb", LibNum: 1}): testBlocks(3, "3bbbbbbbbbbbbbbb", "2bbbbbbbbbbbbbbb", 1),
				BlockFileName(&Block{Id: "2bbbbbbbbbbbbbbb", Number: 2, PreviousId: "1aaaaaaaaaaaaaaa", LibNum: 1}): testBlocks(2, "2bbbbbbbbbbbbbbb", "1aaaaaaaaaaaaaaa", 1),
			},
		},
		{
			"undo cursor",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepUndo,
				Block:     NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				HeadBlock: NewBlockRef("4bbbbbbbbbbbbbbb", 4),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 1),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			map[string][]byte{
				BlockFileName(&Block{Id: "3bbbbbbbbbbbbbbb", Number: 3, PreviousId: "2aaaaaaaaaaaaaaa", LibNum: 1}): testBlocks(3, "3bbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaa", 1),
			},
		},
		{
			"undo cursor same",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepUndo,
				Block:     NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				HeadBlock: NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 1),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			nil,
		},
		{
			"undo cursor same with holes",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaa", 1,
				6, "6aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepUndo,
				Block:     NewBlockRef("4aaaaaaaaaaaaaaa", 4),
				HeadBlock: NewBlockRef("4aaaaaaaaaaaaaaa", 4),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 1),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "6aaaaaaaaaaaaaaa", num: 6},
					step: StepNewIrreversible,
				},
			},
			nil,
		},
		{
			"newirr cursor",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			Cursor{
				Step:      StepNewIrreversible,
				Block:     NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				HeadBlock: NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				LIB:       NewBlockRef("3aaaaaaaaaaaaaaa", 3),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			map[string][]byte{
				BlockFileName(&Block{Id: "3bbbbbbbbbbbbbbb", Number: 3, PreviousId: "2aaaaaaaaaaaaaaa", LibNum: 1}): testBlocks(3, "3bbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaa", 1),
			},
		},
	}

	type resp struct {
		blk  string
		step string
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			merged := dstore.NewMockStore(nil)
			merged.SetFile(base(0), test.blocks)

			forked := dstore.NewMockStore(nil)
			for k, v := range test.forkedBlocks {
				forked.SetFile(k, v)
			}

			var received []resp
			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
				received = append(received, resp{
					blk.String(),
					obj.(Stepable).Step().String(),
				})
				if len(received) == len(test.expected) {
					return errDone
				}
				return nil
			})

			fs := NewFileSourceFromCursor(merged, forked, &test.cursor, handler, zlog)
			testDone := make(chan struct{})
			go func() {
				fs.Run()
				assert.Equal(t, errDone, fs.Err())
				close(testDone)
			}()
			select {
			case <-testDone:
			case <-time.After(100 * time.Millisecond):
				t.Error("Test timeout")
			}
			var expectedStrings []resp
			for _, exp := range test.expected {
				expectedStrings = append(expectedStrings, resp{
					exp.blk.String(),
					exp.step.String(),
				})
			}
			assert.Equal(t, expectedStrings, received)
			assert.ErrorIs(t, fs.Err(), errDone)
		})
	}
}

func TestCursorThroughResolver(t *testing.T) {

	cases := []struct {
		name         string
		blocks       []byte
		startBlock   uint64
		cursor       Cursor
		expected     []blockWithStep
		expectError  bool
		forkedBlocks map[string][]byte
	}{
		{
			"valid cursor",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 2,
			),
			2,
			Cursor{
				Step:      StepNew,
				Block:     NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				HeadBlock: NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 2),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			false,
			nil,
		},
		{
			"undo valid cursor same behavior",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 2,
			),
			2,
			Cursor{
				Step:      StepUndo,
				Block:     NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				HeadBlock: NewBlockRef("3aaaaaaaaaaaaaaa", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 2),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
			},
			false,
			nil,
		},
		{
			"send right away up to LIB",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 2,
				5, "5aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
				6, "6aaaaaaaaaaaaaaa", "6aaaaaaaaaaaaaaa", 2,
			),
			2,
			Cursor{
				Step:      StepNew,
				Block:     NewBlockRef("9aaaaaaaaaaaaaaa", 9),
				HeadBlock: NewBlockRef("9aaaaaaaaaaaaaaa", 9),
				LIB:       NewBlockRef("5aaaaaaaaaaaaaaa", 5),
			},
			[]blockWithStep{
				{
					blk:  &BasicBlockRef{id: "2aaaaaaaaaaaaaaa", num: 2},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "3aaaaaaaaaaaaaaa", num: 3},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
					step: StepNewIrreversible,
				},
				{
					blk:  &BasicBlockRef{id: "5aaaaaaaaaaaaaaa", num: 5},
					step: StepNewIrreversible,
				},
			},
			false,
			nil,
		},
		{
			"invalid filesource cursor NOT IMPLEMENTED",
			testBlocks(
				1, "1aaaaaaaaaaaaaaa", "", 0,
				2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
				3, "3aaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaa", 1,
				4, "4aaaaaaaaaaaaaaa", "3aaaaaaaaaaaaaaa", 2,
				5, "5aaaaaaaaaaaaaaa", "4aaaaaaaaaaaaaaa", 2,
			),
			2,
			Cursor{
				Step:      StepNew,
				Block:     NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				HeadBlock: NewBlockRef("3bbbbbbbbbbbbbbb", 3),
				LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 1),
			},
			nil,
			true,
			nil,
		},
	}

	type resp struct {
		blk  string
		step string
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			merged := dstore.NewMockStore(nil)
			merged.SetFile(base(0), test.blocks)

			forked := dstore.NewMockStore(nil)
			for k, v := range test.forkedBlocks {
				forked.SetFile(k, v)
			}

			var received []resp
			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
				received = append(received, resp{
					blk.String(),
					obj.(Stepable).Step().String(),
				})
				if len(received) == len(test.expected) {
					return errDone
				}
				return nil
			})

			fs := NewFileSourceThroughCursor(merged, forked, test.startBlock, &test.cursor, handler, zlog)
			testDone := make(chan struct{})
			go func() {
				fs.Run()
				if test.expectError {
					assert.NotEqual(t, errDone, fs.Err())
				} else {
					assert.Equal(t, errDone, fs.Err())
				}
				close(testDone)
			}()
			select {
			case <-testDone:
			case <-time.After(100 * time.Millisecond):
				t.Error("Test timeout")
			}
			if test.expectError {
				return
			}
			var expectedStrings []resp
			for _, exp := range test.expected {
				expectedStrings = append(expectedStrings, resp{
					exp.blk.String(),
					exp.step.String(),
				})
			}
			assert.Equal(t, expectedStrings, received)
			assert.ErrorIs(t, fs.Err(), errDone)
		})
	}
}

func TestCursorResolverWithHoles(t *testing.T) {
	merged := dstore.NewMockStore(nil)
	merged.SetFile(base(0), testBlocks(
		1, "1aaaaaaaaaaaaaaa", "", 0,
		2, "2aaaaaaaaaaaaaaa", "1aaaaaaaaaaaaaaa", 1,
		4, "4aaaaaaaaaaaaaaa", "2aaaaaaaaaaaaaaa", 2,
	))

	forked := dstore.NewMockStore(nil)
	forked.SetFile(BlockFileName(&Block{
		Id:         "3bbbbbbbbbbbbbbb",
		Number:     3,
		PreviousId: "2aaaaaaaaaaaaaaa",
		LibNum:     1,
	}), testBlocks(3, "3bbbbbbbbbbbbbbb", "2aaaaaaaaaaaaaaa", 1),
	)

	expected := []blockWithStep{
		{
			blk:  &BasicBlockRef{id: "3bbbbbbbbbbbbbbb", num: 3},
			step: StepUndo,
		},
		{
			blk:  &BasicBlockRef{id: "4aaaaaaaaaaaaaaa", num: 4},
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
		Block:     NewBlockRef("3bbbbbbbbbbbbbbb", 3),
		HeadBlock: NewBlockRef("3bbbbbbbbbbbbbbb", 3),
		LIB:       NewBlockRef("1aaaaaaaaaaaaaaa", 2),
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
