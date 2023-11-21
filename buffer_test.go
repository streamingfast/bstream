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

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/stretchr/testify/assert"
)

func TestBufferCreate(t *testing.T) {
	b := NewBuffer("bob", zlog)
	assert.NotNil(t, b.elements)
	assert.NotNil(t, b.list)
}

func TestBufferOperations(t *testing.T) {

	tests := []struct {
		name           string
		operations     func(b *Buffer)
		expectedBlocks []*pbbstream.Block
		expectedMap    map[string]*pbbstream.Block
	}{
		{
			"add a few",
			func(b *Buffer) {
				b.AppendHead(TestBlock("00000002a", "00000001a"))
				b.AppendHead(TestBlock("00000003a", "00000002a"))
			},
			[]*pbbstream.Block{
				TestBlock("00000002a", "00000001a"),
				TestBlock("00000003a", "00000002a"),
			},
			map[string]*pbbstream.Block{
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
			[]*pbbstream.Block{
				TestBlock("00000003a", "00000002a"),
			},
			map[string]*pbbstream.Block{
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
			[]*pbbstream.Block{
				TestBlock("00000002a", "00000001a"),
				TestBlock("00000004a", "00000003a"),
			},
			map[string]*pbbstream.Block{
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
			[]*pbbstream.Block{
				TestBlock("00000004a", "00000003a"),
				TestBlock("00000005a", "00000004a"),
			},
			map[string]*pbbstream.Block{
				"00000004a": TestBlock("00000004a", "00000003a"),
				"00000005a": TestBlock("00000005a", "00000004a"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := NewBuffer(test.name, zlog)
			test.operations(b)
			assert.Equal(t, test.expectedBlocks, b.AllBlocks())

			assert.Equal(t, len(test.expectedMap), len(b.elements))
			for id, val := range test.expectedMap {
				el, ok := b.elements[id]
				assert.True(t, ok)
				if ok {
					assert.Equal(t, val, el.Value)
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
				assert.Equal(t, []*pbbstream.Block{
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
				assert.Equal(t, []*pbbstream.Block{
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
