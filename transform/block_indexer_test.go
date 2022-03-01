package transform

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/streamingfast/dstore"
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
