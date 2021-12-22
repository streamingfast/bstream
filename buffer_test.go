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
	b := NewBuffer("bob", zlog)
	assert.NotNil(t, b.elements)
	assert.NotNil(t, b.list)
}

func TestBufferOperations(t *testing.T) {

	tests := []struct {
		name        string
		operations  func(b *Buffer)
		expectedMap map[string]BlockRef
	}{
		{
			name: "add a few",
			operations: func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
			},
			expectedMap: map[string]BlockRef{
				"00000002a": TestBlock("00000002a", "00000001a"),
				"00000003a": TestBlock("00000003a", "00000002a"),
			},
		},
		{
			name: "poptail",
			operations: func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
				b.PopTail()
			},
			expectedMap: map[string]BlockRef{
				"00000003a": TestBlock("00000003a", "00000002a"),
			},
		},
		{
			name: "delete",
			operations: func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
				b.AppendHead(TestBlock("00000004a", "00000003a"))
				b.Delete(TestBlock("00000003a", "00000002a"))
			},
			expectedMap: map[string]BlockRef{
				"00000002a": TestBlock("00000002a", "00000001a"),
				"00000004a": TestBlock("00000004a", "00000003a"),
			},
		},
		{
			name: "truncateTail",
			operations: func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
				b.AppendHead(TestBlock("00000004a", "00000003a"))
				b.AppendHead(TestBlock("00000005a", "00000004a"))
				b.TruncateTail(3)
			},
			expectedMap: map[string]BlockRef{
				"00000004a": TestBlock("00000004a", "00000003a"),
				"00000005a": TestBlock("00000005a", "00000004a"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer := NewBuffer(test.name, zlog)
			test.operations(buffer)

			assert.Equal(t, len(test.expectedMap), len(buffer.elements))
			for id, expectedVal := range test.expectedMap {
				el, ok := buffer.elements[id]
				assert.True(t, ok)
				if ok {
					assert.Equal(t, expectedVal.ID(), el.Value.(BlockRef).ID())
				}

			}
		})
	}
}

func TestBufferLookups(t *testing.T) {

	var newFilledBuffer = func() *Buffer {
		b := NewBuffer("test lookups", zlog)
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
				assert.Equal(t, "00000002a", tail.ID())
			},
		},
		{
			"head",
			func(b *Buffer) {
				head := b.Head()
				assert.Equal(t, "00000005a", head.ID())
			},
		},
		{
			"get by ID",
			func(b *Buffer) {
				blk := b.GetByID("00000004a")
				assert.Equal(t, "00000004a", blk.ID())
			},
		},
		{
			"poptail value",
			func(b *Buffer) {
				tail := b.PopTail()
				assert.Equal(t, "00000002a", tail.ID())
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
				assert.Equal(t, []string{"00000002a", "00000003a", "00000004a"}, []string{tail[0].ID(), tail[1].ID(), tail[2].ID()})
			},
		},
		{
			"head blocks",
			func(b *Buffer) {
				head := b.HeadBlocks(4)
				assert.Equal(t, []string{"00000002a", "00000003a", "00000004a", "00000005a"}, []string{head[0].ID(), head[1].ID(), head[2].ID(), head[3].ID()})
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
