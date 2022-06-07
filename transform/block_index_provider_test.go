package transform

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/streamingfast/dstore"
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
			// we provide our naive filterFunc inline
			indexProvider := NewGenericBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, func(getFunc BitmapGetter) (matchingBlocks []uint64) {
				var results []uint64
				for _, desired := range test.lookingFor {
					if bitmap := getFunc(desired); bitmap != nil {
						slice := bitmap.ToArray()[:]
						results = append(results, slice...)
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

func TestBlockIndexProvider_FindIndexContaining(t *testing.T) {
	tests := []struct {
		name           string
		blocks         []map[uint64][]string
		indexSize      uint64
		indexShortname string
		lowBlockNum    uint64
	}{
		{
			name:           "sunny path",
			blocks:         testBlockValues(t, 5),
			indexSize:      2,
			indexShortname: "test",
			lowBlockNum:    10,
		},
		// froch // make multi tests instead of complex test logic
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// populate a mock dstore with some index files
			indexStore := testMockstoreWithFiles(t, test.blocks, test.indexSize)

			// spawn an indexProvider with the populated dstore
			indexProvider := NewGenericBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, func(getFunc BitmapGetter) (matchingBlocks []uint64) {
				return nil
			})
			require.NotNil(t, indexProvider)

			// try to load an index without finding it first
			err := indexProvider.loadIndex(io.NopCloser(strings.NewReader("bogus")), test.lowBlockNum, test.indexSize)
			require.Error(t, err)

			ctx := context.Background()

			// try to find indices with non-existent block nums
			r, lowBlockNum, indexSize := indexProvider.findIndexContaining(ctx, 42)
			require.Nil(t, r)
			require.Equal(t, uint64(0), lowBlockNum)
			require.Equal(t, uint64(0), indexSize)
			r, lowBlockNum, indexSize = indexProvider.findIndexContaining(ctx, 69)
			require.Nil(t, r)
			require.Equal(t, uint64(0), lowBlockNum)
			require.Equal(t, uint64(0), indexSize)

			// find the index containing a known block num
			r, lowBlockNum, indexSize = indexProvider.findIndexContaining(ctx, 10)
			require.NotNil(t, r)
			require.Equal(t, lowBlockNum, lowBlockNum)
			require.Equal(t, indexSize, indexSize)

			// load the index we found, and ensure it's valid
			err = indexProvider.loadIndex(r, lowBlockNum, indexSize)
			require.Nil(t, err)
			require.Equal(t, indexSize, indexProvider.currentIndex.indexSize)
			require.Equal(t, lowBlockNum, indexProvider.currentIndex.lowBlockNum)

			// find the index containing a known block num, from another index file
			r, lowBlockNum, indexSize = indexProvider.findIndexContaining(ctx, 12)
			require.NotNil(t, r)
			require.Equal(t, lowBlockNum, indexProvider.currentIndex.lowBlockNum+indexSize)
			require.Equal(t, indexSize, indexProvider.currentIndex.indexSize)

			// load the index we found, and ensure it's valid
			err = indexProvider.loadIndex(r, lowBlockNum, indexSize)
			require.Nil(t, err)
			require.Equal(t, lowBlockNum, indexProvider.currentIndex.lowBlockNum)
			require.Equal(t, indexSize, indexProvider.currentIndex.indexSize)
		})
	}
}

func TestBlockIndexProvider_WithinRange(t *testing.T) {
	tests := []struct {
		name           string
		blocks         []map[uint64][]string
		indexSize      uint64
		indexShortname string
		lowBlockNum    uint64
		wantedBlock    uint64
		isWithinRange  bool
	}{
		{
			name:           "block exists in first index",
			blocks:         testBlockValues(t, 5),
			indexSize:      2,
			indexShortname: "test",
			lowBlockNum:    0,
			wantedBlock:    11,
			isWithinRange:  true,
		},
		{
			name:           "block exists in second index",
			blocks:         testBlockValues(t, 5),
			indexSize:      2,
			indexShortname: "test",
			lowBlockNum:    0,
			wantedBlock:    13,
			isWithinRange:  true,
		},
		{
			name:           "block doesn't exist",
			blocks:         testBlockValues(t, 5),
			indexSize:      2,
			indexShortname: "test",
			lowBlockNum:    0,
			wantedBlock:    69,
			isWithinRange:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// populate a mock dstore with some index files
			indexStore := testMockstoreWithFiles(t, test.blocks, test.indexSize)

			// spawn an indexProvider with the populated dstore
			indexProvider := NewGenericBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, func(getFunc BitmapGetter) (matchingBlocks []uint64) {
				return nil
			})
			require.NotNil(t, indexProvider)

			// call loadRange on known blocks
			b := indexProvider.WithinRange(context.Background(), test.wantedBlock)
			if test.isWithinRange {
				require.True(t, b)
			} else {
				require.False(t, b)
			}
		})
	}
}

func TestBlockIndexProvider_Matches(t *testing.T) {
	tests := []struct {
		name           string
		blocks         []map[uint64][]string
		indexSize      uint64
		indexShortname string
		lowBlockNum    uint64
		wantedBlock    uint64
		lookingFor     []string
		expectMatches  bool
		filterFunc     func(BitmapGetter) []uint64
	}{
		{
			name:           "matches",
			blocks:         testBlockValues(t, 5),
			indexSize:      2,
			indexShortname: "test",
			lowBlockNum:    0,
			wantedBlock:    11,
			expectMatches:  true,
			filterFunc: func(_ BitmapGetter) []uint64 {
				return []uint64{11}
			},
		},
		{
			name:           "doesn't match",
			blocks:         testBlockValues(t, 5),
			indexSize:      2,
			indexShortname: "test",
			lowBlockNum:    0,
			wantedBlock:    11,
			expectMatches:  false,
			filterFunc: func(_ BitmapGetter) []uint64 {
				return []uint64{69}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexStore := testMockstoreWithFiles(t, test.blocks, test.indexSize)
			indexProvider := NewGenericBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, test.filterFunc)

			b, err := indexProvider.Matches(context.Background(), test.wantedBlock)
			require.NoError(t, err)
			if test.expectMatches {
				require.True(t, b)
			} else {
				require.False(t, b)
			}
		})
	}
}

func TestBlockIndexProvider_NextMatching(t *testing.T) {
	tests := []struct {
		name                        string
		blocks                      []map[uint64][]string
		indexSize                   uint64
		indexShortname              string
		lowBlockNum                 uint64
		wantedBlock                 uint64
		lookingFor                  []string
		expectedNextBlockNum        uint64
		expectedPassedIndexBoundary bool
		expectedMatchingBlocksLen   int
	}{
		{
			name:                        "block exists in first index and filters also match block in second index",
			blocks:                      testBlockValues(t, 5),
			indexSize:                   2,
			indexShortname:              "test",
			lowBlockNum:                 0,
			wantedBlock:                 11,
			lookingFor:                  []string{"cccccccccccccccccccccccccccccccccccccccc"},
			expectedNextBlockNum:        13,
			expectedPassedIndexBoundary: false,
			expectedMatchingBlocksLen:   1,
		},
		{
			name:                        "block exists in first index and filters also match block outside bounds",
			blocks:                      testBlockValues(t, 5),
			indexSize:                   2,
			indexShortname:              "test",
			lowBlockNum:                 0,
			wantedBlock:                 11,
			lookingFor:                  []string{"3333333333333333333333333333333333333333"},
			expectedNextBlockNum:        14,
			expectedPassedIndexBoundary: true,
			expectedMatchingBlocksLen:   0,
		},
		{
			name:                        "filters don't match any known blocks",
			blocks:                      testBlockValues(t, 5),
			indexSize:                   2,
			indexShortname:              "test",
			lowBlockNum:                 0,
			wantedBlock:                 11,
			lookingFor:                  []string{"0xDEADBEEF"},
			expectedNextBlockNum:        14,
			expectedPassedIndexBoundary: true,
			expectedMatchingBlocksLen:   0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// populate a mock dstore with some index files
			indexStore := testMockstoreWithFiles(t, test.blocks, test.indexSize)

			// spawn an indexProvider
			// we provide our naive filterFunc inline
			indexProvider := NewGenericBlockIndexProvider(indexStore, test.indexShortname, []uint64{test.indexSize}, func(getFunc BitmapGetter) (matchingBlocks []uint64) {
				var results []uint64
				for _, desired := range test.lookingFor {
					if bitmap := getFunc(desired); bitmap != nil {
						slice := bitmap.ToArray()[:]
						results = append(results, slice...)
					}
				}
				return results
			})

			nextBlockNum, passedIndexBoundary, err := indexProvider.NextMatching(context.Background(), test.wantedBlock, 0)
			require.NoError(t, err)
			require.Equal(t, passedIndexBoundary, test.expectedPassedIndexBoundary)
			require.Equal(t, test.expectedNextBlockNum, nextBlockNum)
			require.Equal(t, test.expectedMatchingBlocksLen, len(indexProvider.currentMatchingBlocks))
		})
	}
}
