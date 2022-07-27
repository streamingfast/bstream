package transform

import (
	"io"
	"testing"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBlockIndexProvider(t *testing.T) {
	indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})
	indexProvider := NewGenericBlockIndexProvider(indexStore, "test", []uint64{10}, func(BitmapGetter) []uint64 {
		return nil
	})
	require.NotNil(t, indexProvider)
	require.IsType(t, GenericBlockIndexProvider{}, *indexProvider)
}

func TestBlockIndexProvider_LoadRange(t *testing.T) {
	tests := []struct {
		name                   string
		blocks                 []map[uint64][]string
		indexSize              uint64
		indexShortname         string
		lowBlockNum            uint64
		lookingFor             []string
		lookingForPrefixes     []string
		lookingForSuffixes     []string
		lookingForBoth         [][2]string
		expectedMatchingBlocks []uint64
	}{
		{
			name:                   "new with matches",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingFor:             []string{"cccccccccccccccccccccccccccccccccccccccc"},
			expectedMatchingBlocks: []uint64{10, 13},
		},
		{
			name:                   "new with single match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingFor:             []string{"dddddddddddddddddddddddddddddddddddddddd"},
			expectedMatchingBlocks: []uint64{11},
		},
		{
			name:                   "new with no matches",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingFor:             []string{"DEADBEEF"},
			expectedMatchingBlocks: nil,
		},
		{
			name:                   "new with prefix matches",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForPrefixes:     []string{"pref"},
			expectedMatchingBlocks: []uint64{10, 12},
		},
		{
			name:                   "new with prefix no match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForPrefixes:     []string{"nada"},
			expectedMatchingBlocks: nil,
		},
		{
			name:                   "new with prefix single match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForPrefixes:     []string{"ddd"},
			expectedMatchingBlocks: []uint64{11},
		},
		{
			name:                   "new with suffix matches",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForSuffixes:     []string{"suffix"},
			expectedMatchingBlocks: []uint64{10, 12},
		},
		{
			name:                   "new with suffix no match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForSuffixes:     []string{"nada"},
			expectedMatchingBlocks: nil,
		},
		{
			name:                   "new with suffix single match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForSuffixes:     []string{"ddd"},
			expectedMatchingBlocks: []uint64{11},
		},
		{
			name:                   "new with prefix AND suffix matches",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForBoth:         [][2]string{{"pref", "suffix"}},
			expectedMatchingBlocks: []uint64{10, 12},
		},
		{
			name:                   "new with prefix AND suffix no match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForBoth:         [][2]string{{"pref", "nada"}},
			expectedMatchingBlocks: nil,
		},
		{
			name:                   "new with prefix AND suffix single match",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForBoth:         [][2]string{{"dddd", "ddd"}},
			expectedMatchingBlocks: []uint64{11},
		},
		{
			name:                   "new multiple matches",
			blocks:                 testBlockValues(t, 6),
			indexSize:              5,
			indexShortname:         "test",
			lowBlockNum:            10,
			lookingForBoth:         [][2]string{{"dddd", "ddd"}, {"111", "111"}},
			expectedMatchingBlocks: []uint64{11, 12},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// populate a mock dstore with some index files
			indexStore := testMockstoreWithFiles(t, test.blocks, test.indexSize)

			// spawn an indexProvider with the populated dstore
			// we provide our naive filterFunc inline
			indexProvider := NewGenericBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, func(bitmaps BitmapGetter) (matchingBlocks []uint64) {
				var results []uint64
				for _, desired := range test.lookingFor {
					if bitmap := bitmaps.Get(desired); bitmap != nil {
						slice := bitmap.ToArray()[:]
						results = append(results, slice...)
					}
				}

				for _, pref := range test.lookingForPrefixes {
					if bitmap := bitmaps.GetByPrefixAndSuffix(pref, ""); bitmap != nil {
						slice := bitmap.ToArray()[:]
						results = append(results, slice...)
					}
				}

				for _, suff := range test.lookingForSuffixes {
					if bitmap := bitmaps.GetByPrefixAndSuffix("", suff); bitmap != nil {
						slice := bitmap.ToArray()[:]
						results = append(results, slice...)
					}
				}

				for _, pair := range test.lookingForBoth {
					if bitmap := bitmaps.GetByPrefixAndSuffix(pair[0], pair[1]); bitmap != nil {
						slice := bitmap.ToArray()[:]
						results = append(results, slice...)
					}
				}

				return results
			})
			require.NotNil(t, indexProvider)

			err := indexProvider.loadRange(test.lowBlockNum, 5)
			require.NoError(t, err)
			assert.Equal(t, test.expectedMatchingBlocks, indexProvider.matchingBlocks)
			assert.Equal(t, test.lowBlockNum, indexProvider.loadedLowBoundary)
			assert.Equal(t, test.lowBlockNum+test.indexSize, indexProvider.loadedExclusiveHighBoundary)
		})
	}
}
