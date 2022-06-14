package transform

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
)

// testMockstoreWithFiles will populate a MockStore with indexes of the provided Blocks, according to the provided indexSize
func testMockstoreWithFiles(t *testing.T, blocks []map[uint64][]string, indexSize uint64) *dstore.MockStore {
	results := make(map[string][]byte)

	// spawn an indexStore which will populate the results
	indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		content, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		results[base] = content
		return nil
	})

	// spawn an indexer with our mock indexStore
	indexer := NewBlockIndexer(indexStore, indexSize, "test")
	for _, blk := range blocks {
		// feed the indexer
		for k, v := range blk {
			indexer.Add(v, k)
		}
	}

	// populate a new indexStore with the prior results
	indexStore = dstore.NewMockStore(nil)
	for indexName, indexContents := range results {
		indexStore.SetFile(indexName, indexContents)
	}

	return indexStore
}

// testBlockValues simulates a a list of blocks, returning a blocknum with payload of potential interest
// it takes a size parameter, to truncate with [:size]
func testBlockValues(t *testing.T, size int) []map[uint64][]string {
	kvs := []map[uint64][]string{
		{
			10: {
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"3333333333333333333333333333333333333333",
				"cccccccccccccccccccccccccccccccccccccccc",
				"prefzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzsuffix",
			},
		},
		{
			11: {
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"dddddddddddddddddddddddddddddddddddddddd",
			},
		},
		{
			12: {
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"1111111111111111111111111111111111111111",
				"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"prefwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwsuffix",
			},
		},
		{
			13: {
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"4444444444444444444444444444444444444444",
				"cccccccccccccccccccccccccccccccccccccccc",
			},
		},
		{
			14: {
				"5555555555555555555555555555555555555555",
				"7777777777777777777777777777777777777777",
				"3333333333333333333333333333333333333333",
			},
		},
		{
			15: {},
		},
	}

	if size != 0 {
		return kvs[:size]
	}
	return kvs
}
