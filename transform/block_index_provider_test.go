package transform

import (
	"context"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewBlockIndexProvider(t *testing.T) {
	indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})
	indexProvider := NewBlockIndexProvider(indexStore, "test", []uint64{10}, func(index *BlockIndex) (matchingBlocks []uint64) {
		return nil
	})
	require.NotNil(t, indexProvider)
	require.IsType(t, BlockIndexProvider{}, *indexProvider)
}

func TestBlockIndexProvider_LoadRange(t *testing.T) {
	tests := []struct {
		name                   string
		blocks                 []map[uint64][]string
		indexSize              uint64
		indexShortname         string
		lowBlockNum            uint64
		lookingFor             []string
		expectedMatchingBlocks []uint64
	}{
		{
			name:                   "new with matches",
			blocks:                 testBlockValues(t, 5),
			indexSize:              2,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingFor:             []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			expectedMatchingBlocks: []uint64{10, 11},
		},
		{
			name:                   "new with single match",
			blocks:                 testBlockValues(t, 5),
			indexSize:              2,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingFor:             []string{"dddddddddddddddddddddddddddddddddddddddd"},
			expectedMatchingBlocks: []uint64{11},
		},
		{
			name:                   "new with no matches",
			blocks:                 testBlockValues(t, 5),
			indexSize:              2,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingFor:             []string{"0xDEADBEEF"},
			expectedMatchingBlocks: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// populate a mock dstore with some index files
			indexStore := testMockstoreWithFiles(t, test.blocks, test.indexSize)

			// spawn an indexProvider with the populated dstore
			// we provide a naive filterFunc inline
			indexProvider := NewBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, func(index *BlockIndex) (matchingBlocks []uint64) {
				var results []uint64
				for key, bitmap := range index.KV() {
					for _, desired := range test.lookingFor {
						if key == desired {
							slice := bitmap.ToArray()[:]
							results = append(results, slice...)
						}
					}
				}
				return results
			})
			require.NotNil(t, indexProvider)

			ctx := context.Background()
			err := indexProvider.loadRange(ctx, test.lowBlockNum)
			require.NoError(t, err)
			require.NotNil(t, indexProvider.currentIndex)
			require.Equal(t, test.expectedMatchingBlocks, indexProvider.currentMatchingBlocks)
		})
	}
}
