package forkable

import (
	"testing"

	"github.com/stretchr/testify/require"
)

//func TestDB_buildTree(t *testing.T) {
//	db := NewForkDB()
//
//	db.AddLink(bTestBlock("3a", "2a"),bTestBlock("2a", "1a"), nil)
//	db.AddLink(bTestBlock("4a", "3a"),bTestBlock("3a", "2a"), nil)
//	db.AddLink(bTestBlock("5a", "4a"),bTestBlock("4a", "3a"), nil)
//	db.AddLink(bTestBlock("6a", "5a"),bTestBlock("5a", "4a"), nil)
//
//	nodeTree, err := db.BuildTree()
//	require.NoError(t, err)
//	fmt.Println(nodeTree)
//}

func TestDB_roots(t *testing.T) {
	db := NewForkDB()

	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000005a"), "00000004a", nil)

	db.AddLink(bRef("00000002b"), "00000001b", nil)

	roots := db.roots()
	require.Equal(t, []string{"00000002a", "00000002b"}, roots)

}
func TestDB_Size(t *testing.T) {
	db := NewForkDB()

	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000005a"), "00000004a", nil)
	db.AddLink(bRef("00000004b"), "00000003a", nil)
	db.AddLink(bRef("00000005b"), "00000004b", nil)
	db.AddLink(bRef("00000006b"), "00000005b", nil)
	db.AddLink(bRef("00000005c"), "00000004b", nil)
	db.AddLink(bRef("00000006c"), "00000005c", nil)

	tree, err := db.BuildTree()
	require.NoError(t, err)
	require.Equal(t, 9, tree.Size())

}

func TestDB_node_chains(t *testing.T) {
	db := NewForkDB()

	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000005a"), "00000004a", nil)

	db.AddLink(bRef("00000003b"), "00000002a", nil)
	db.AddLink(bRef("00000004b"), "00000003b", nil)

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
	db := NewForkDB()
	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000004b"), "00000003a", nil)
	db.AddLink(bRef("00000005b"), "00000004b", nil)

	nodeTree, err := db.BuildTree()
	require.NoError(t, err)

	chain := nodeTree.Chains()

	require.Equal(t, []string{"00000002a", "00000003a", "00000004b", "00000005b"}, chain.LongestChain())
}
func TestDB_MultipleLongestChain(t *testing.T) {
	db := NewForkDB()
	db.AddLink(bRef("00000002a"), "00000001a", nil)
	db.AddLink(bRef("00000003a"), "00000002a", nil)
	db.AddLink(bRef("00000004a"), "00000003a", nil)
	db.AddLink(bRef("00000004b"), "00000003a", nil)
	//db.AddLink(bRef("00000005b"), "00000004b", nil)

	nodeTree, err := db.BuildTree()
	require.NoError(t, err)

	chain := nodeTree.Chains()

	require.Equal(t, nil, chain.LongestChain())
}
