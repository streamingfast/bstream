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

	db.AddLink(bTestBlock("00000003a", "00000002a"), bTestBlock("00000002a", "00000001a"), nil)
	db.AddLink(bTestBlock("00000004a", "00000003a"), bTestBlock("00000003a", "00000002a"), nil)
	db.AddLink(bTestBlock("00000005a", "00000004a"), bTestBlock("00000004a", "00000003a"), nil)
	db.AddLink(bTestBlock("00000006a", "00000005a"), bTestBlock("00000005a", "00000004a"), nil)

	db.AddLink(bTestBlock("00000003b", "00000002a"), bTestBlock("00000002a", "00000001a"), nil)

	roots := db.roots()
	require.Equal(t, []string{"00000003a", "00000003b"}, roots)
}

func TestDB_node_chains(t *testing.T) {
	db := NewForkDB()

	db.AddLink(bTestBlock("00000003a", "00000002a"), bTestBlock("00000002a", "00000001a"), nil)
	db.AddLink(bTestBlock("00000004a", "00000003a"), bTestBlock("00000003a", "00000002a"), nil)
	db.AddLink(bTestBlock("00000005a", "00000004a"), bTestBlock("00000004a", "00000003a"), nil)
	db.AddLink(bTestBlock("00000006a", "00000005a"), bTestBlock("00000005a", "00000004a"), nil)

	db.AddLink(bTestBlock("00000004b", "00000003b"), bTestBlock("00000003a", "00000002a"), nil)
	db.AddLink(bTestBlock("00000005b", "00000004b"), bTestBlock("00000004b", "00000003b"), nil)
	db.AddLink(bTestBlock("00000006b", "00000005b"), bTestBlock("00000005b", "00000004b"), nil)

	nodeTree, err := db.BuildTree()
	require.NoError(t, err)

	chains := &chainList{
		chains: [][]string{},
	}
	nodeTree.chains(nil, chains)

	expected := &chainList{
		chains: [][]string{
			{"00000003a", "00000004a", "00000005a", "00000006a"},
			{"00000003a", "00000004b", "00000005b", "00000006b"},
		},
	}

	require.Equal(t, expected, chains)
}

//
//func TestDB_LongestChain(t *testing.T) {
//	db := NewDB()
//	db.AddLink("2a", "1a", nil)
//	db.AddLink("3a", "2a", nil)
//	db.AddLink("4a", "3a", nil)
//	db.AddLink("4b", "3a", nil)
//	db.AddLink("5b", "4b", nil)
//
//	chain, err := db.LongestChain()
//	require.NoError(t, err)
//
//	require.Equal(t, []string{"1a", "2a", "3a", "4b", "5b"}, chain)
//}
