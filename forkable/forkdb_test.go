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
	"testing"

	"github.com/dfuse-io/bstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddLinkSimple(t *testing.T) {
	f := NewForkDB()
	f.AddLink(bRef("00000001a"), bRef("00000000b"), []string{"tx1", "tx2"})
	assert.Equal(t, f.links, map[string]string{"00000001a": "00000000b"})
	assert.Equal(t, f.nums, map[string]uint64{"00000001a": 1, "00000000b": 0})
	assert.Equal(t, f.objects, map[string]interface{}{"00000001a": []string{"tx1", "tx2"}})
}

func TestOutOfChain(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000000a"))

	require.False(t, f.AddLink(bRef("00000004b"), bRef("00000003b"), nil))
	require.False(t, f.AddLink(bRef("00000005b"), bRef("00000004b"), nil))
	require.False(t, f.AddLink(bRef("00000006b"), bRef("00000005b"), nil))
	seg := f.ReversibleSegment(bRef("00000005b"))
	require.Len(t, seg, 0)
}

func TestNoLIBNoAnswer(t *testing.T) {
	f := NewForkDB()
	require.False(t, f.AddLink(bRef("00000004a"), bRef("00000003a"), nil))
	require.False(t, f.AddLink(bRef("00000005a"), bRef("00000004a"), nil))
	require.False(t, f.AddLink(bRef("00000005b"), bRef("00000004a"), nil))

	require.Panics(t, func() {
		f.ReversibleSegment(bRef("00000005b"))
	})

	seg := f.stalledInSegment(nil)
	require.Empty(t, seg)

	undo, redo := f.ChainSwitchSegments("head_block_id", "00000005b")
	require.Empty(t, undo)
	require.Empty(t, redo)

	require.Empty(t, f.LIBID())
	require.Zero(t, f.LIBNum())
}

func TestImplicitBlock1Irreversible(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	assert.False(t, f.AddLink(bRef("00000002a"), bRef("00000001a"), nil))
	assert.False(t, f.AddLink(bRef("00000003a"), bRef("00000002a"), nil))
	assert.False(t, f.AddLink(bRef("00000004a"), bRef("00000003a"), nil))
	els := f.ReversibleSegment(bRef("00000003a"))
	assert.Len(t, els, 2)
}

func TestAddLinkExists(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	assert.False(t, f.AddLink(bRef("00000002a"), bRef("00000001a"), nil))
	assert.True(t, f.AddLink(bRef("00000002a"), bRef("00000001x"), nil))
}

func TestPurgeHeads(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	//  /-E  /- H
	// A <- B <- C <- D
	//   `- F <- G

	f.AddLink(bRef("00000001a"), bEmptyRef, nil)
	f.AddLink(bRef("00000004a"), bRef("00000003a"), nil)
	f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000005a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000006a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000007a"), bRef("00000006a"), nil)
	f.AddLink(bRef("00000008a"), bRef("00000007a"), nil)

	f.MoveLIB(bRef("00000001a"))
	blocks := f.ReversibleSegment(bRef("00000001a"))
	assert.Len(t, blocks, 0)

	blocks = f.ReversibleSegment(bRef("00000003a"))
	assert.Len(t, blocks, 2)

	f.MoveLIB(bRef("000000ffa"))
	blocks = f.ReversibleSegment(bRef("00000003a"))
	assert.Len(t, blocks, 0)
}

func TestIrreversibleSegment(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))
	f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)

	irreversibleSegment := f.ReversibleSegment(bRef("00000002a"))
	require.Len(t, irreversibleSegment, 1)
	require.Equal(t, "00000002a", irreversibleSegment[0].BlockID)

	f.MoveLIB(bRef("00000002a"))
	f.AddLink(bRef("00000003b"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000004a"), bRef("00000003a"), nil)
	f.AddLink(bRef("00000005a"), bRef("00000004a"), nil)

	irreversibleSegment = f.ReversibleSegment(bRef("00000004a"))
	require.Len(t, irreversibleSegment, 2)
	require.Equal(t, "00000003a", irreversibleSegment[0].BlockID)
	require.Equal(t, "00000004a", irreversibleSegment[1].BlockID)

}

func TestStalledInSegment(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	// 1    2    3    4    5
	//   ,- E ,- H
	// A <- B <- C <- D <- I
	//   `- F <- G

	f.AddLink(bRef("00000001a"), bEmptyRef, nil)
	f.AddLink(bRef("00000002e"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000002b"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000002f"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003c"), bRef("00000002b"), nil)
	f.AddLink(bRef("00000004d"), bRef("00000003c"), nil)
	f.AddLink(bRef("00000003g"), bRef("00000002f"), nil)
	f.AddLink(bRef("00000003h"), bRef("00000002b"), nil)
	f.AddLink(bRef("00000005i"), bRef("00000004d"), nil)

	blocks := f.ReversibleSegment(bRef("00000005i"))
	assert.Len(t, blocks, 4)
	assert.Equal(t, "00000002b", blocks[0].BlockID)
	assert.Equal(t, "00000003c", blocks[1].BlockID)
	assert.Equal(t, "00000004d", blocks[2].BlockID)
	assert.Equal(t, "00000005i", blocks[3].BlockID)

	stale := f.stalledInSegment(blocks)
	assert.Len(t, stale, 4)
	assert.Equal(t, "00000002e", stale[0].BlockID)
	assert.Equal(t, "00000002f", stale[1].BlockID)
	assert.Equal(t, "00000003g", stale[2].BlockID)
	assert.Equal(t, "00000003h", stale[3].BlockID)

}

func TestIsBehindLIB(t *testing.T) {
	fdb := NewForkDB()
	fdb.InitLIB(bRef("00000002"))
	fdb.AddLink(bRef("00000002"), bRef("00000001"), nil)
	fdb.AddLink(bRef("00000003"), bRef("00000002"), nil)

	assert.True(t, fdb.IsBehindLIB(1))
	assert.True(t, fdb.IsBehindLIB(2))
	assert.False(t, fdb.IsBehindLIB(3))
}

func TestChainSwitchSegments(t *testing.T) {
	tests := []struct {
		setupForkdb        func() *ForkDB
		name               string
		headBlockID        string
		newBlockPreviousID string
		expectedUndo       []string
		expectedRedo       []string
	}{
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				return f
			},
			name:               "00000002a",
			headBlockID:        "",
			newBlockPreviousID: "00000001a",
		},
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
				return f
			},
			name:               "00000003a",
			headBlockID:        "00000002a",
			newBlockPreviousID: "00000002a",
		},
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
				f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
				return f
			},
			name:               "00000003c",
			headBlockID:        "00000003a",
			newBlockPreviousID: "00000002a",
			expectedUndo:       []string{"00000003a"},
		},
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
				f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
				f.AddLink(bRef("00000003c"), bRef("00000002a"), nil)
				return f
			},
			name:               "00000004c",
			headBlockID:        "00000003a",
			newBlockPreviousID: "00000003c",
			expectedUndo:       []string{"00000003a"},
			expectedRedo:       []string{"00000003c"},
		},
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
				f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
				f.AddLink(bRef("00000003c"), bRef("00000002a"), nil)
				f.AddLink(bRef("00000004c"), bRef("00000003c"), nil)
				f.AddLink(bRef("00000004b"), bRef("00000003b"), nil)
				f.AddLink(bRef("00000005b"), bRef("00000004b"), nil)
				f.AddLink(bRef("00000005c"), bRef("00000004c"), nil)
				f.AddLink(bRef("00000004a"), bRef("00000003a"), nil)
				f.AddLink(bRef("00000005a"), bRef("00000004a"), nil)
				return f
			},
			name:               "00000006a",
			headBlockID:        "00000005c",
			newBlockPreviousID: "00000005a",
			expectedUndo:       []string{"00000005c", "00000004c", "00000003c"},
			expectedRedo:       []string{"00000003a", "00000004a", "00000005a"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := test.setupForkdb()
			undo, redo := f.ChainSwitchSegments(test.headBlockID, test.newBlockPreviousID)
			assert.Equal(t, test.expectedUndo, undo, "Undo segment")
			assert.Equal(t, test.expectedRedo, redo, "Redo segment")
		})
	}
}

func TestBlockForID(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	f.AddLink(bRef("00000001a"), bEmptyRef, "1a")
	f.AddLink(bRef("00000002b"), bRef("00000001a"), "2b")

	assert.Equal(t, &Block{
		BlockID:         "00000002b",
		BlockNum:        2,
		PreviousBlockID: "00000001a",
		Object:          "2b",
	}, f.BlockForID("00000002b"))

	assert.Nil(t, f.BlockForID("ffffffffa"))
}

func TestBlockInCurrentChain(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	f.AddLink(bRef("00000002b"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003c"), bRef("00000002b"), nil)
	f.AddLink(bRef("00000004d"), bRef("00000003c"), nil)
	f.AddLink(bRef("00000005e"), bEmptyRef, nil)

	tests := []struct {
		headID      bstream.BlockRef
		blockNum    uint64
		expectedRef bstream.BlockRef
	}{
		{
			headID:      bRef("00000003c"),
			blockNum:    2,
			expectedRef: bRef("00000002b"),
		},
		{
			headID:      bRef("00000004d"),
			blockNum:    2,
			expectedRef: bRef("00000002b"),
		},
		{
			headID:      bRef("00000005e"),
			blockNum:    2,
			expectedRef: bEmptyRef,
		},
	}

	for _, test := range tests {
		s := f.BlockInCurrentChain(test.headID, test.blockNum)

		assert.Equal(t, test.expectedRef.ID(), s.ID())
		assert.Equal(t, test.expectedRef.Num(), s.Num())
	}
}

func TestMoveLIB(t *testing.T) {
	fdb := NewForkDB()
	fdb.InitLIB(bRef("00000001a"))

	var cases = []struct {
		name           string
		purgeBelow     bstream.BlockRef
		expectedLinks  int
		expectedNums   int
		expectedBlocks int
	}{
		{
			name:       "clean below 7",
			purgeBelow: bRef("00000007z"),
		},
		{
			name:           "clean below 6",
			purgeBelow:     bRef("00000006a"),
			expectedLinks:  1,
			expectedNums:   2,
			expectedBlocks: 1,
		},
		{
			name:           "clean below 4",
			purgeBelow:     bRef("00000004a"),
			expectedLinks:  4,
			expectedNums:   5,
			expectedBlocks: 4,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fdb.AddLink(bRef("00000002a"), bRef("00000001a"), "")
			fdb.AddLink(bRef("00000003a"), bRef("00000002a"), "")
			fdb.AddLink(bRef("00000003b"), bRef("00000002a"), "")
			fdb.AddLink(bRef("00000003c"), bRef("00000002a"), "")
			fdb.AddLink(bRef("00000004a"), bRef("00000003a"), "")
			fdb.AddLink(bRef("00000005a"), bRef("00000004a"), "")
			fdb.AddLink(bRef("00000005b"), bRef("00000004a"), "")
			fdb.AddLink(bRef("00000006a"), bRef("00000005a"), "")

			fdb.MoveLIB(c.purgeBelow)

			assert.Equal(t, c.expectedLinks, len(fdb.links))
			assert.Equal(t, c.expectedNums, len(fdb.nums))
			assert.Equal(t, c.expectedBlocks, len(fdb.objects))
		})
	}
}

func TestNewIrreversibleSegment(t *testing.T) {
	fdb := NewForkDB()
	fdb.InitLIB(bRef("00000001a"))

	fdb.AddLink(bRef("00000002a"), bRef("00000001a"), "")
	fdb.AddLink(bRef("00000003a"), bRef("00000002a"), "")
	fdb.AddLink(bRef("00000003c"), bRef("00000002a"), "")

	segment := fdb.ReversibleSegment(bRef("00000003a"))
	assert.Len(t, segment, 2)

	assert.Equal(t, "00000002a", segment[0].BlockID)
	assert.Equal(t, "00000003a", segment[1].BlockID)
}

func TestLIBID(t *testing.T) {
	b1 := bTestBlock("00000001a", "00000000a")
	fdb := NewForkDB()
	fdb.InitLIB(b1)

	b2 := bTestBlockWithLIBNum("00000002a", "00000001a", 1)
	b3 := bTestBlockWithLIBNum("00000003a", "00000002a", 1)

	fdb.AddLink(b1, bEmptyRef, nil)
	fdb.AddLink(b2, b1, nil)
	fdb.AddLink(b3, b2, nil)

	fdb.MoveLIB(b2)
	assert.Equal(t, b2.ID(), fdb.LIBID())
	assert.Equal(t, b2.Num(), fdb.LIBNum())

	assert.Equal(t, fdb.nums, map[string]uint64{"00000003a": 3, "00000002a": 2, "00000001a": 1})
}
