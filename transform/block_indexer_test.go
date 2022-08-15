package transform

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBlockIndexer(t *testing.T) {
	indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})
	indexer := NewBlockIndexer(indexStore, 10, "")
	require.NotNil(t, indexer)
	require.IsType(t, BlockIndexer{}, *indexer)
	require.Equal(t, "default", indexer.indexShortname)
}

func Test_FindNextUnindexed(t *testing.T) {

	tests := []struct {
		indexSizes      []uint64
		files           []string
		startBlock      uint64
		firstStreamable uint64
		expectNext      uint64
	}{
		{
			indexSizes:      []uint64{10000, 1000},
			files:           []string{"0000020000.10000.test.idx", "000030000.1000.test.idx", "000031000.1000.test.idx", "009999000.1000.test.idx"},
			startBlock:      22222,
			firstStreamable: 0,
			expectNext:      32000,
		},
		{
			indexSizes:      []uint64{10000},
			files:           []string{"0000020000.10000.test.idx", "000030000.1000.test.idx", "000031000.1000.test.idx", "009999000.1000.test.idx"},
			startBlock:      22222,
			firstStreamable: 0,
			expectNext:      30000,
		},
		{
			indexSizes:      []uint64{10000, 1000},
			files:           []string{"0000020000.10000.test.idx", "000030000.1000.test.idx", "000031000.1000.test.idx", "009999000.1000.test.idx"},
			startBlock:      33000,
			firstStreamable: 0,
			expectNext:      33000,
		},
		{
			indexSizes:      []uint64{10000, 1000},
			files:           []string{"0000020000.10000.test.idx", "000030000.1000.test.idx", "000031000.1000.test.idx", "009999000.1000.test.idx"},
			startBlock:      0,
			firstStreamable: 20000,
			expectNext:      32000,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			bstream.GetProtocolFirstStreamableBlock = test.firstStreamable
			indexStore := dstore.NewMockStore(nil)
			for _, name := range test.files {
				indexStore.SetFile(name, nil)
			}
			ctx := context.Background()
			next := FindNextUnindexed(ctx, test.startBlock, test.indexSizes, "test", indexStore)
			assert.EqualValues(t, int(test.expectNext), int(next))
		})
	}
}

func TestBlockIndexer_String(t *testing.T) {
	indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})
	indexer := NewBlockIndexer(indexStore, 10, "")
	str := indexer.String()
	require.NotNil(t, str)

}

func TestBlockIndexer_writeIndex(t *testing.T) {
	tests := []struct {
		name              string
		indexSize         uint64
		indexShortname    string
		expectError       bool
		expectedResultLen int
		kv                map[uint64][]string
	}{
		{
			name:              "sunny path",
			indexSize:         10,
			indexShortname:    "test",
			expectError:       false,
			expectedResultLen: 1,
			kv: map[uint64][]string{
				10: {
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"cccccccccccccccccccccccccccccccccccccccc",
				},
				11: {
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
			},
		},
		{
			name:              "expect error due to nil index; lowBlockNum % indexSize != 0",
			indexSize:         3,
			indexShortname:    "test",
			expectError:       true,
			expectedResultLen: 1,
			kv: map[uint64][]string{
				10: {
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"cccccccccccccccccccccccccccccccccccccccc",
				},
				11: {
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
					"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			results := make(map[string][]byte)

			// spawn a dstore
			indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
				content, err := ioutil.ReadAll(f)
				require.NoError(t, err)
				results[base] = content
				return nil
			})

			// spawn an indexer and feed it
			indexer := NewBlockIndexer(indexStore, test.indexSize, "test")
			for blockNum, keys := range test.kv {
				indexer.Add(keys, blockNum)
			}

			// write the index to dstore
			err := indexer.writeIndex()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// check the contents
				require.Equal(t, test.expectedResultLen, len(results))
				for k, _ := range results {
					require.Equal(t, k, toIndexFilename(test.indexSize, 10, test.indexShortname))
				}
			}
		})
	}
}
