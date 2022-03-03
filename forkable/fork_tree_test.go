package forkable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDB_BuildTree(t *testing.T) {
	cases := []struct {
		name          string
		db            func() *ForkDB
		expectedSize  int
		expectedError error
	}{
		{
			name: "Sunny path",
			db: func() *ForkDB {
				db := NewForkDB(1)
				db.AddLink(bRef("00000002a"), "00000001a", nil)
				db.AddLink(bRef("00000003a"), "00000002a", nil)
				db.AddLink(bRef("00000004a"), "00000003a", nil)
				db.AddLink(bRef("00000005a"), "00000004a", nil)
				db.AddLink(bRef("00000004b"), "00000003a", nil)
				db.AddLink(bRef("00000005b"), "00000004b", nil)
				db.AddLink(bRef("00000006b"), "00000005b", nil)
				db.AddLink(bRef("00000005c"), "00000004b", nil)
				db.AddLink(bRef("00000006c"), "00000005c", nil)
				db.libRef = bRef("00000002a")
				return db
			},
			expectedSize:  9,
			expectedError: nil,
		},
		{
			name: "Lib in the middle",
			db: func() *ForkDB {
				db := NewForkDB(1)
				db.AddLink(bRef("00000002a"), "00000001a", nil)
				db.AddLink(bRef("00000003a"), "00000002a", nil)
				db.AddLink(bRef("00000004a"), "00000003a", nil)
				db.AddLink(bRef("00000005a"), "00000004a", nil)
				db.AddLink(bRef("00000004b"), "00000003a", nil)
				db.AddLink(bRef("00000005b"), "00000004b", nil)
				db.AddLink(bRef("00000006b"), "00000005b", nil)
				db.AddLink(bRef("00000005c"), "00000004b", nil)
				db.AddLink(bRef("00000006c"), "00000005c", nil)
				db.libRef = bRef("00000005a")
				return db
			},
			expectedSize: 9,
		},
		{
			name: "Missing Lib",
			db: func() *ForkDB {
				db := NewForkDB(1)
				db.AddLink(bRef("00000002a"), "00000001a", nil)
				db.AddLink(bRef("00000003a"), "00000002a", nil)
				db.AddLink(bRef("00000004a"), "00000003a", nil)
				db.AddLink(bRef("00000005a"), "00000004a", nil)
				db.AddLink(bRef("00000004b"), "00000003a", nil)
				db.AddLink(bRef("00000005b"), "00000004b", nil)
				db.AddLink(bRef("00000006b"), "00000005b", nil)
				db.AddLink(bRef("00000005c"), "00000004b", nil)
				db.AddLink(bRef("00000006c"), "00000005c", nil)
				db.libRef = bRef("00000003b")
				return db
			},
			expectedSize:  0,
			expectedError: RootNotFound,
		},
		{
			name: "Multiple root",
			db: func() *ForkDB {
				db := NewForkDB(1)
				db.AddLink(bRef("00000002a"), "00000001a", nil)
				db.AddLink(bRef("00000003a"), "00000002a", nil)
				db.AddLink(bRef("00000004a"), "00000003a", nil)

				db.AddLink(bRef("00000002b"), "00000001b", nil)
				db.AddLink(bRef("00000003b"), "00000002b", nil)

				db.AddLink(bRef("00000002c"), "00000001c", nil)
				db.AddLink(bRef("00000003c"), "00000002c", nil)
				db.AddLink(bRef("00000004c"), "00000003c", nil)
				db.AddLink(bRef("00000005c"), "00000004c", nil)
				db.AddLink(bRef("00000006c"), "00000005c", nil)
				db.libRef = bRef("00000002a")
				return db
			},
			expectedSize: 03,
		},
		{
			name: "No lib set",
			db: func() *ForkDB {
				db := NewForkDB(1)
				db.AddLink(bRef("00000002a"), "00000001a", nil)
				return db
			},
			expectedError: RootNotFound,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := c.db()
			tree, err := db.BuildTree()
			require.Equal(t, c.expectedError, err)
			if c.expectedError != nil {
				return
			}
			require.Equal(t, c.expectedSize, tree.Size())
		})
	}

}

func TestDB_node_chains(t *testing.T) {
	db := NewForkDB(1)

	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000005a"), "00000004a", nil)

	db.AddLink(bRef("00000003b"), "00000002a", nil)
	db.AddLink(bRef("00000004b"), "00000003b", nil)

	db.libRef = bRef("00000002a")

	nodeTree, err := db.BuildTree()
	require.NoError(t, err)

	chains := &ChainList{
		Chains: [][]string{},
	}
	nodeTree.chains(nil, chains)

	expected := &ChainList{
		Chains: [][]string{
			{"00000002a", "00000003a", "00000004a", "00000005a"},
			{"00000002a", "00000003b", "00000004b"},
		},
	}

	require.Equal(t, expected, chains)
}

func TestDB_LongestChain(t *testing.T) {

	cases := []struct {
		name                 string
		db                   func() *ForkDB
		expectedLongestChain []string
	}{
		{
			name: "Sunny path",
			db: func() *ForkDB {
				db := NewForkDB(1)
				db.libRef = bRef("00000002a")
				db.AddLink(bRef("00000002a"), "00000001a", nil)
				db.AddLink(bRef("00000003a"), "00000002a", nil)
				db.AddLink(bRef("00000004a"), "00000003a", nil)
				db.AddLink(bRef("00000004b"), "00000003a", nil)
				db.AddLink(bRef("00000005b"), "00000004b", nil)
				return db
			},
			expectedLongestChain: []string{"00000002a", "00000003a", "00000004b", "00000005b"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := c.db()
			nodeTree, err := db.BuildTree()
			require.NoError(t, err)
			chains := nodeTree.Chains()
			require.Equal(t, c.expectedLongestChain, chains.LongestChain())
		})
	}
}

func TestDB_MultipleLongestChain(t *testing.T) {
	db := NewForkDB(1)
	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000004b"), "00000003a", nil)
	//db.AddLink(bRef("00000005b"), "00000004b", nil)
	db.libRef = bRef("00000002a")

	nodeTree, err := db.BuildTree()
	require.NoError(t, err)

	chain := nodeTree.Chains()

	require.Len(t, chain.LongestChain(), 0)
}
