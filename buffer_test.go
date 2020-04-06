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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferCreate(t *testing.T) {
	b := NewBuffer("bob")
	assert.Equal(t, "bob", b.name)
	assert.NotNil(t, b.elements)
	assert.NotNil(t, b.list)
}

func TestBufferOperations(t *testing.T) {

	tests := []struct {
		name           string
		operations     func(b *Buffer)
		expectedBlocks []BlockRef
		expectedMap    map[string]BlockRef
	}{
		{
			"add a few",
			func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
			},
			[]BlockRef{
				TestBlock("00000002a", "00000001a"),
				TestBlock("00000003a", "00000002a"),
			},
			map[string]BlockRef{
				"00000002a": TestBlock("00000002a", "00000001a"),
				"00000003a": TestBlock("00000003a", "00000002a"),
			},
		},
		{
			"poptail",
			func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
				b.PopTail()
			},
			[]BlockRef{
				TestBlock("00000003a", "00000002a"),
			},
			map[string]BlockRef{
				"00000003a": TestBlock("00000003a", "00000002a"),
			},
		},
		{
			"delete",
			func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
				b.AppendHead(TestBlock("00000004a", "00000003a"))
				b.Delete(TestBlock("00000003a", "00000002a"))
			},
			[]BlockRef{
				TestBlock("00000002a", "00000001a"),
				TestBlock("00000004a", "00000003a"),
			},
			map[string]BlockRef{
				"00000002a": TestBlock("00000002a", "00000001a"),
				"00000004a": TestBlock("00000004a", "00000003a"),
			},
		},
		{
			"truncateTail",
			func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
				b.AppendHead(TestBlock("00000004a", "00000003a"))
				b.AppendHead(TestBlock("00000005a", "00000004a"))
				b.TruncateTail(3)
			},
			[]BlockRef{
				TestBlock("00000004a", "00000003a"),
				TestBlock("00000005a", "00000004a"),
			},
			map[string]BlockRef{
				"00000004a": TestBlock("00000004a", "00000003a"),
				"00000005a": TestBlock("00000005a", "00000004a"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := NewBuffer(test.name)
			test.operations(b)
			assert.Equal(t, test.expectedBlocks, b.AllBlocks())

			assert.Equal(t, len(test.expectedMap), len(b.elements))
			for id, val := range test.expectedMap {
				el, ok := b.elements[id]
				assert.True(t, ok)
				if ok {
					assert.Equal(t, val, el.Value.(BlockRef))
				}

			}
		})
	}
}

func TestBufferLookups(t *testing.T) {

	var newFilledBuffer = func() *Buffer {
		b := NewBuffer("test lookups")
		b.AppendHead(TestBlock("00000002a", "00000001a"))
		b.AppendHead(TestBlock("00000003a", "00000002a"))
		b.AppendHead(TestBlock("00000004a", "00000003a"))
		b.AppendHead(TestBlock("00000005a", "00000004a"))
		return b
	}

	tests := []struct {
		name   string
		assert func(b *Buffer)
	}{
		{
			"len",
			func(b *Buffer) {
				assert.Equal(t, 4, b.Len())
			},
		},
		{
			"tail",
			func(b *Buffer) {
				tail := b.Tail()
				assert.Equal(t, TestBlock("00000002a", "00000001a"), tail)
			},
		},
		{
			"head",
			func(b *Buffer) {
				head := b.Head()
				assert.Equal(t, TestBlock("00000005a", "00000004a"), head)
			},
		},
		{
			"get by ID",
			func(b *Buffer) {
				blk := b.GetByID("00000004a")
				assert.Equal(t, TestBlock("00000004a", "00000003a"), blk)
			},
		},
		{
			"poptail value",
			func(b *Buffer) {
				tail := b.PopTail()
				assert.Equal(t, TestBlock("00000002a", "00000001a"), tail)
			},
		},
		{
			"exists",
			func(b *Buffer) {
				assert.False(t, b.Exists("00000001a"))
				assert.True(t, b.Exists("00000002a"))
				assert.True(t, b.Exists("00000003a"))
				assert.True(t, b.Exists("00000004a"))
				assert.True(t, b.Exists("00000005a"))
			},
		},
		{
			"contains",
			func(b *Buffer) {
				assert.False(t, b.Contains(1))
				assert.True(t, b.Contains(2))
				assert.True(t, b.Contains(3))
				assert.True(t, b.Contains(4))
				assert.True(t, b.Contains(5))
			},
		},
		{
			"truncate_tail",
			func(b *Buffer) {
				tail := b.TruncateTail(4)
				assert.Equal(t, []BlockRef{
					TestBlock("00000002a", "00000001a"),
					TestBlock("00000003a", "00000002a"),
					TestBlock("00000004a", "00000003a"),
				}, tail)
			},
		},
		{
			"head blocks",
			func(b *Buffer) {
				head := b.HeadBlocks(4)
				assert.Equal(t, []BlockRef{
					TestBlock("00000002a", "00000001a"),
					TestBlock("00000003a", "00000002a"),
					TestBlock("00000004a", "00000003a"),
					TestBlock("00000005a", "00000004a"),
				}, head)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := newFilledBuffer()
			test.assert(b)
		})
	}
}

//
//
//
//
//	b.AppendHead(TestBlock("00000002a", "00000001a"))
//	assert.Equal(t, 1, b.Len())
//	assert.Equal(t, "00000002a", b.Tail().ID())
//	assert.Equal(t, "00000002a", b.Head().ID())
//
//	b.AppendHead(TestBlock("00000003a", "00000002a"))
//	assert.Equal(t, "00000002a", b.Tail().ID())
//	assert.Equal(t, "00000003a", b.Head().ID())
//	assert.Equal(t, 2, b.Len())
//	assert.True(t, b.IsFull())
//	assert.Equal(t, "00000002a", b.GetByID("00000002a").ID())
//
//	blks := b.AllBlocks()
//	assert.Equal(t, 2, len(blks))
//	assert.Equal(t, "00000002a", blks[0].ID())
//	assert.Equal(t, "00000003a", blks[1].ID())
//
//	ref := b.PopTail()
//	assert.Equal(t, "00000002a", ref.ID())
//
//	assert.Nil(t, b.GetByID("00000002a"))
//	assert.Equal(t, "00000003a", b.GetByID("00000003a").ID())
//
//	b.AppendHead(TestBlock("00000004a", "00000003a"))
//	b.AppendHead(TestBlock("00000005a", "00000004a"))
//
//	head := b.HeadBlocks(999)
//	assert.Equal(t, 3, len(head))
//
//	head = b.HeadBlocks(2)
//	assert.Equal(t, 2, len(head))
//	assert.Equal(t, "00000004a", head[0].ID())
//	assert.Equal(t, "00000005a", head[1].ID())
//
//	truncated := b.TruncateTail(4)
//	assert.Equal(t, 2, len(truncated))
//	assert.Equal(t, "00000003a", truncated[0].ID())
//	assert.Equal(t, "00000004a", truncated[1].ID())
//	assert.Equal(t, 1, b.Len())
//}
