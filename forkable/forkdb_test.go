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

func TestForkDB_OutOfChain(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000000a"))

	require.False(t, f.AddLink(bRef("00000004b"), bRef("00000003b"), nil))
	require.False(t, f.AddLink(bRef("00000005b"), bRef("00000004b"), nil))
	require.False(t, f.AddLink(bRef("00000006b"), bRef("00000005b"), nil))
	seg, err := f.reversibleSegment(bRef("00000005b"))
	require.NoError(t, err)

	require.Len(t, seg, 0)
}

func TestForkDB_ReversibleSegment_UsualCase(t *testing.T) {
	fdb, _, _, _, _, _ := newUsualCaseForkDB()
	nextLIBRef := bRefInSegment(fdb.libRef.Num()+12, "aa")

	seg, err := fdb.reversibleSegment(nextLIBRef)
	require.NoError(t, err)
	require.Len(t, seg, 12)
}

func TestForkDB_NoLIBNoAnswer(t *testing.T) {
	f := NewForkDB()

	//          /---> 5b
	//  3a --> 4a --> 5a
	//
	require.False(t, f.AddLink(bRef("00000004a"), bRef("00000003a"), nil))
	require.False(t, f.AddLink(bRef("00000005a"), bRef("00000004a"), nil))
	require.False(t, f.AddLink(bRef("00000005b"), bRef("00000004a"), nil))

	_, err := f.reversibleSegment(bRef("00000005b"))
	require.EqualError(t, err, "the LIB ID is not defined and should have been")

	seg := f.stalledSegmentsInNodes(nil, true)
	require.Empty(t, seg)

	undo, redo := f.chainSwitchSegments(bRef("00000001a"), bRef("00000005b"))
	require.Empty(t, undo)
	require.Empty(t, redo)

	require.Nil(t, f.libRef)
}

func TestForkDB_ImplicitBlock1Irreversible(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	assert.False(t, f.AddLink(bRef("00000001a"), nil, nil))
	assert.False(t, f.AddLink(bRef("00000002a"), bRef("00000001a"), nil))
	assert.False(t, f.AddLink(bRef("00000003a"), bRef("00000002a"), nil))
	assert.False(t, f.AddLink(bRef("00000004a"), bRef("00000003a"), nil))
	els, err := f.reversibleSegment(bRef("00000003a"))
	require.NoError(t, err)
	require.Len(t, els, 2)
}

func TestForkDB_AddLinkExists(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	assert.False(t, f.AddLink(bRef("00000001a"), nil, nil))
	assert.False(t, f.AddLink(bRef("00000002a"), bRef("00000001a"), nil))
	assert.True(t, f.AddLink(bRef("00000002a"), bRef("00000001x"), nil))
}

func TestForkDB_PurgeHeads(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	//      /---> 5a
	//      |
	//  -- 1a --> 2a --> 3a --> 4a
	//      |
	//      \---> 6a --> 7a --> 8a
	//
	f.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, nil)
	f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000004a"), bRef("00000003a"), nil)
	f.AddLink(bRef("00000005a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000006a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000007a"), bRef("00000006a"), nil)
	f.AddLink(bRef("00000008a"), bRef("00000007a"), nil)

	f.MoveLIB(bRef("00000001a"))
	nodes, err := f.reversibleSegment(bRef("00000001a"))
	require.NoError(t, err)
	require.Len(t, nodes, 0)

	nodes, err = f.reversibleSegment(bRef("00000003a"))
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	// Using `MoveLIB` is not working here since `MoveLIB` only accepts known blocks
	f.InitLIB(bRef("000000ffa"))
	nodes, err = f.reversibleSegment(bRef("00000003a"))
	require.NoError(t, err)
	require.Len(t, nodes, 0)
}

func TestForkDB_IrreversibleSegment(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	f.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, nil)
	f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)

	irreversibleSegment, err := f.reversibleSegment(bRef("00000002a"))
	require.NoError(t, err)
	require.Len(t, irreversibleSegment, 1)
	require.Equal(t, "00000002a", irreversibleSegment[0].ID())

	f.MoveLIB(bRef("00000002a"))
	f.AddLink(bRef("00000003b"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000004a"), bRef("00000003a"), nil)
	f.AddLink(bRef("00000005a"), bRef("00000004a"), nil)

	irreversibleSegment, err = f.reversibleSegment(bRef("00000004a"))
	require.NoError(t, err)

	require.Len(t, irreversibleSegment, 2)
	require.Equal(t, "00000003a", irreversibleSegment[0].ID())
	require.Equal(t, "00000004a", irreversibleSegment[1].ID())

}

func TestForkDB_StalledSegmentsInNodes(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	//             /-------> 3e
	//             |
	//             |          /---> 4h
	//             |          |
	//             +-------> 3b --> 4c --> 5d --> 6i
	//  -- 1a --> 2a
	//             |
	//             \-------> 3f --> 4g
	//
	f.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, nil)
	f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003e"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000003b"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000003f"), bRef("00000002a"), nil)
	f.AddLink(bRef("00000004c"), bRef("00000003b"), nil)
	f.AddLink(bRef("00000005d"), bRef("00000004c"), nil)
	f.AddLink(bRef("00000004g"), bRef("00000003f"), nil)
	f.AddLink(bRef("00000004h"), bRef("00000003b"), nil)
	f.AddLink(bRef("00000006i"), bRef("00000005d"), nil)

	nodes, err := f.reversibleSegment(bRef("00000006i"))
	require.NoError(t, err)

	require.Len(t, nodes, 5)
	assert.Equal(t, bRef("00000002a"), nodes[0].ref)
	assert.Equal(t, bRef("00000003b"), nodes[1].ref)
	assert.Equal(t, bRef("00000004c"), nodes[2].ref)
	assert.Equal(t, bRef("00000005d"), nodes[3].ref)
	assert.Equal(t, bRef("00000006i"), nodes[4].ref)

	stale := f.stalledSegmentsInNodes(nodes, true)
	require.Len(t, stale, 4)
	assert.Equal(t, "00000003e", stale[0].ID())
	assert.Equal(t, "00000003f", stale[1].ID())
	assert.Equal(t, "00000004g", stale[2].ID())
	assert.Equal(t, "00000004h", stale[3].ID())
}

func TestForkDB_IsBehindLIB(t *testing.T) {
	fdb := NewForkDB()
	fdb.InitLIB(bRef("00000002"))
	fdb.AddLink(bRef("00000002"), bRef("00000001"), nil)
	fdb.AddLink(bRef("00000003"), bRef("00000002"), nil)

	assert.True(t, fdb.IsBehindLIB(1))
	assert.True(t, fdb.IsBehindLIB(2))
	assert.False(t, fdb.IsBehindLIB(3))
}

func TestForkDB_ChainSwitchSegments(t *testing.T) {
	tests := []struct {
		setupForkdb          func() *ForkDB
		name                 string
		oldHeadBlock         string
		newHeadPreviousBlock string
		expectedUndo         []string
		expectedRedo         []string
	}{
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				return f
			},
			name:                 "00000002a",
			oldHeadBlock:         "000000000",
			newHeadPreviousBlock: "00000001a",
		},
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
				return f
			},
			name:                 "00000003a",
			oldHeadBlock:         "00000002a",
			newHeadPreviousBlock: "00000002a",
		},
		{
			setupForkdb: func() *ForkDB {
				f := NewForkDB()
				f.InitLIB(bRef("00000001a"))

				f.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
				f.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
				return f
			},
			name:                 "00000003c",
			oldHeadBlock:         "00000003a",
			newHeadPreviousBlock: "00000002a",
			expectedUndo:         []string{"00000003a"},
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
			name:                 "00000004c",
			oldHeadBlock:         "00000003a",
			newHeadPreviousBlock: "00000003c",
			expectedUndo:         []string{"00000003a"},
			expectedRedo:         []string{"00000003c"},
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
			name:                 "00000006a",
			oldHeadBlock:         "00000005c",
			newHeadPreviousBlock: "00000005a",
			expectedUndo:         []string{"00000005c", "00000004c", "00000003c"},
			expectedRedo:         []string{"00000003a", "00000004a", "00000005a"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := test.setupForkdb()
			undo, redo := f.chainSwitchSegments(bRef(test.oldHeadBlock), bRef(test.newHeadPreviousBlock))

			toRefs := func(ids []string) (refs []bstream.BlockRef) {
				if len(ids) == 0 {
					return nil
				}

				refs = make([]bstream.BlockRef, len(ids))
				for i, id := range ids {
					refs[i] = bRef(id)
				}
				return
			}

			assert.Equal(t, toRefs(test.expectedUndo), undo, "Undo segment")
			assert.Equal(t, toRefs(test.expectedRedo), redo, "Redo segment")
		})
	}
}

func TestForkDB_LinkForRef(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	f.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, "1a")
	f.AddLink(bRef("00000002b"), bRef("00000001a"), "2b")

	node1a := &node{ref: bRef("00000001a"), obj: "1a"}
	node2b := &node{ref: bRef("00000002b"), prev: node1a, obj: "2b"}
	node1a.nexts = append(node1a.nexts, node2b)

	link := f.nodeForRef(bRef("00000002b"))
	assert.Equal(t, node2b, link)

	link = f.nodeForRef(bRef("ffffffffa"))
	assert.Nil(t, link)
}

func TestForkDB_BlockInCurrentChain(t *testing.T) {
	f := NewForkDB()
	f.InitLIB(bRef("00000001a"))

	f.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, nil)
	f.AddLink(bRef("00000002b"), bRef("00000001a"), nil)
	f.AddLink(bRef("00000003c"), bRef("00000002b"), nil)
	f.AddLink(bRef("00000004d"), bRef("00000003c"), nil)
	f.AddLink(bRef("00000005e"), bstream.BlockRefEmpty, nil)

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
			expectedRef: bstream.BlockRefEmpty,
		},
	}

	for _, test := range tests {
		s := f.BlockInCurrentChain(test.headID, test.blockNum)

		assert.Equal(t, test.expectedRef.ID(), s.ID())
		assert.Equal(t, test.expectedRef.Num(), s.Num())
	}
}

func TestForkDB_MoveLIB(t *testing.T) {
	var cases = []struct {
		name          string
		purgeBelow    bstream.BlockRef
		expectedNodes int
		expectedErr   error
	}{
		// {
		// 	name:          "clean below 6",
		// 	purgeBelow:    bRef("00000006a"),
		// 	expectedNodes: 1,
		// },
		{
			name:          "clean below 5",
			purgeBelow:    bRef("00000005a"),
			expectedNodes: 2,
		},
		// {
		// 	name:          "clean below 4",
		// 	purgeBelow:    bRef("00000004a"),
		// 	expectedNodes: 4,
		// },
		// {
		// 	name:          "clean below 2",
		// 	purgeBelow:    bRef("00000002a"),
		// 	expectedNodes: 8,
		// },
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			//           /----> 3b     /----> 5b
			//           |             |
			// -- 1a --> 2a --> 3a --> 4a --> 5a --> 6a
			//           |
			//           \----> 3c
			fdb := NewForkDB()
			fdb.InitLIB(bRef("00000001a"))

			fdb.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, "")
			fdb.AddLink(bRef("00000002a"), bRef("00000001a"), "")
			fdb.AddLink(bRef("00000003a"), bRef("00000002a"), "")
			fdb.AddLink(bRef("00000003b"), bRef("00000002a"), "")
			fdb.AddLink(bRef("00000003c"), bRef("00000002a"), "")
			fdb.AddLink(bRef("00000004a"), bRef("00000003a"), "")
			fdb.AddLink(bRef("00000005a"), bRef("00000004a"), "")
			fdb.AddLink(bRef("00000005b"), bRef("00000004a"), "")
			fdb.AddLink(bRef("00000006a"), bRef("00000005a"), "")

			err := fdb.MoveLIB(c.purgeBelow)
			if c.expectedErr == nil {
				require.NoError(t, err)
				require.Len(t, fdb.chain.nodes, c.expectedNodes)
			} else {
				assert.Equal(t, c.expectedErr, err)
			}
		})
	}
}

func TestForkDB_MoveLIB_NoForks(t *testing.T) {
	fdb := NewForkDB()
	fdb.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, nil)
	fdb.AddLink(bRef("00000002a"), bRef("00000001a"), nil)
	fdb.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
	fdb.InitLIB(bRef("00000001a"))

	fdb.MoveLIB(bRef("00000002a"))
	assert.Equal(t, bRef("00000002a"), fdb.libRef)

	b2Node := &node{ref: bRef("00000002a"), prev: nil}
	b3Node := &node{ref: bRef("00000003a"), prev: b2Node}
	b2Node.nexts = []*node{b3Node}

	require.Len(t, fdb.chain.nodes, 2)
	assert.Equal(t, b2Node, fdb.chain.nodes["00000002a"])
	assert.Equal(t, b3Node, fdb.chain.nodes["00000003a"])
}

func TestForkDB_MoveLIB_NewLIB_NotFound(t *testing.T) {
	fdb, _ := newFilledLinear(6)

	err := fdb.MoveLIB(bRef("00000001cc"))
	require.EqualError(t, err, `unable to find new lib node "#1 (00000001cc)"`)
}

func TestForkDB_MoveLIB_NoPreviousLIB(t *testing.T) {
	fdb, head := newFilledLinear(6)
	fdb.libRef = bstream.BlockRefEmpty

	err := fdb.MoveLIB(head)
	require.NoError(t, err)

	assert.Equal(t, head, fdb.libRef)
	require.Len(t, fdb.chain.nodes, 1)
	assert.Equal(t, &node{ref: head}, fdb.chain.nodes[head.ID()])
}

func TestForkDB_MoveLIB_UsualCase_ToHead(t *testing.T) {
	fdb, aaHead, _, _, _, _ := newUsualCaseForkDB()

	err := fdb.MoveLIB(prevRef(aaHead))
	require.NoError(t, err)

	aaLIBNode := &node{ref: fdb.libRef, prev: nil}
	aaHeadNode := &node{ref: aaHead, prev: aaLIBNode}
	eeNode1 := &node{ref: bRefInSegment(aaHead.Num()+1, "ee"), prev: aaHeadNode}
	eeNode2 := &node{ref: nextRef(eeNode1), prev: eeNode1}
	eeNode3 := &node{ref: nextRef(eeNode2), prev: eeNode2}
	eeNode4 := &node{ref: nextRef(eeNode3), prev: eeNode3}
	eeNode5 := &node{ref: nextRef(eeNode4), prev: eeNode4}
	eeNode6 := &node{ref: nextRef(eeNode5), prev: eeNode5}

	aaLIBNode.nexts = []*node{aaHeadNode}
	aaHeadNode.nexts = []*node{eeNode1}
	eeNode1.nexts = []*node{eeNode2}
	eeNode2.nexts = []*node{eeNode3}
	eeNode3.nexts = []*node{eeNode4}
	eeNode4.nexts = []*node{eeNode5}
	eeNode5.nexts = []*node{eeNode6}

	require.Len(t, fdb.chain.nodes, 8)
	assert.Equal(t, aaLIBNode, fdb.chain.nodes[aaLIBNode.ref.ID()])
	assert.Equal(t, aaHeadNode, fdb.chain.nodes[aaHeadNode.ref.ID()])
	assert.Equal(t, eeNode1, fdb.chain.nodes[eeNode1.ref.ID()])
	assert.Equal(t, eeNode2, fdb.chain.nodes[eeNode2.ref.ID()])
	assert.Equal(t, eeNode3, fdb.chain.nodes[eeNode3.ref.ID()])
	assert.Equal(t, eeNode4, fdb.chain.nodes[eeNode4.ref.ID()])
	assert.Equal(t, eeNode5, fdb.chain.nodes[eeNode5.ref.ID()])
	assert.Equal(t, eeNode6, fdb.chain.nodes[eeNode6.ref.ID()])
}

func TestForkDB_NewIrreversibleSegment(t *testing.T) {
	fdb := NewForkDB()
	fdb.InitLIB(bRef("00000001a"))

	fdb.AddLink(bRef("00000001a"), bstream.BlockRefEmpty, "")
	fdb.AddLink(bRef("00000002a"), bRef("00000001a"), "")
	fdb.AddLink(bRef("00000003a"), bRef("00000002a"), "")
	fdb.AddLink(bRef("00000003c"), bRef("00000002a"), "")

	segment, err := fdb.reversibleSegment(bRef("00000003a"))
	require.NoError(t, err)

	require.Len(t, segment, 2)
	assert.Equal(t, bRef("00000002a"), segment[0].ref)
	assert.Equal(t, bRef("00000003a"), segment[1].ref)
}
