package hub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testOneBlockFiles(t *testing.T, count int) (store *dstore.MockStore, filenames []string, ids []string) {
	t.Helper()

	store = dstore.NewMockStore(nil)
	for i := range count {
		blk := bstream.TestBlockWithLIBNum(fmt.Sprintf("%08d", i+2), fmt.Sprintf("%08d", i+1), 1)
		AddToMockStore(t, store, blk)
		filenames = append(filenames, bstream.BlockFileName(blk))
		ids = append(ids, blk.Id)
	}
	return
}

func TestDecodeOneBlocksInOrder(t *testing.T) {
	for _, concurrency := range []int{1, 8} {
		t.Run(fmt.Sprintf("concurrency %d", concurrency), func(t *testing.T) {
			store, filenames, expectedIDs := testOneBlockFiles(t, 40)

			position := make(map[string]int)
			for i, filename := range filenames {
				position[filename] = i
			}

			var lock sync.Mutex
			inFlight, maxInFlight := 0, 0
			store.OpenObjectFunc = func(ctx context.Context, name string) (io.ReadCloser, error) {
				lock.Lock()
				inFlight++
				maxInFlight = max(maxInFlight, inFlight)
				lock.Unlock()
				defer func() {
					lock.Lock()
					inFlight--
					lock.Unlock()
				}()

				// earlier files take longer, so downloads complete out of order
				time.Sleep(time.Duration(len(filenames)-position[name]) * time.Millisecond)
				return io.NopCloser(bytes.NewReader(store.Files[name])), nil
			}

			var processedIDs []string
			err := decodeOneBlocksInOrder(context.Background(), store, filenames, concurrency, func(blk *pbbstream.Block) error {
				processedIDs = append(processedIDs, blk.Id)
				return nil
			})
			require.NoError(t, err)

			assert.Equal(t, expectedIDs, processedIDs)
			assert.LessOrEqual(t, maxInFlight, concurrency)
			if concurrency > 1 {
				assert.Greater(t, maxInFlight, 1)
			}
		})
	}
}

func TestDecodeOneBlocksInOrder_StopsAtDownloadError(t *testing.T) {
	store, filenames, _ := testOneBlockFiles(t, 20)
	store.SetFile(filenames[5], []byte("err"))

	processed := 0
	err := decodeOneBlocksInOrder(context.Background(), store, filenames, 4, func(*pbbstream.Block) error {
		processed++
		return nil
	})

	require.ErrorContains(t, err, filenames[5])
	assert.Equal(t, 5, processed)
}

func TestDecodeOneBlocksInOrder_StopsAtProcessError(t *testing.T) {
	store, filenames, _ := testOneBlockFiles(t, 20)
	errStop := errors.New("stop")

	processed := 0
	err := decodeOneBlocksInOrder(context.Background(), store, filenames, 4, func(*pbbstream.Block) error {
		processed++
		if processed == 3 {
			return errStop
		}
		return nil
	})

	require.ErrorIs(t, err, errStop)
	assert.Equal(t, 3, processed)
}

func TestForkableHub_ReplayedBlocksAndOneBlockStoreLookup(t *testing.T) {
	tests := []struct {
		name            string
		blockToProcess  *pbbstream.Block
		expectedLookups int
	}{
		{
			name:            "block on a fork the hub already holds",
			blockToProcess:  bstream.TestBlockWithLIBNum("00000011b", "00000010", 3),
			expectedLookups: 0,
		},
		{
			name:            "block on the chain the hub already holds",
			blockToProcess:  bstream.TestBlockWithLIBNum("00000012", "00000011a", 3),
			expectedLookups: 0,
		},
		{
			name:            "block whose parent the hub does not hold",
			blockToProcess:  bstream.TestBlockWithLIBNum("00000014", "00000013", 3),
			expectedLookups: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := dstore.NewMockStore(nil)
			AddToMockStore(t, store,
				bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
				bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
				bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
				bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
				bstream.TestBlockWithLIBNum("00000009", "00000008", 3),
				bstream.TestBlockWithLIBNum("00000010", "00000009", 3),
				bstream.TestBlockWithLIBNum("00000011a", "00000010", 3),
				bstream.TestBlockWithLIBNum("00000011b", "00000010", 3),
				bstream.TestBlockWithLIBNum("00000012", "00000011a", 3),
			)

			fh := NewForkableHub(nil, 0, store)
			require.NoError(t, fh.bootstrap())

			lookups := 0
			store.WalkFromFunc = func(ctx context.Context, prefix, startingPoint string, f func(filename string) error) error {
				lookups++
				return nil
			}

			require.NoError(t, fh.ProcessBlock(test.blockToProcess, nil))
			assert.Equal(t, test.expectedLookups, lookups)
		})
	}
}

func TestForkableHub_UnknownOneBlockFiles(t *testing.T) {
	fh := NewForkableHub(nil, 0, dstore.NewMockStore(nil))

	blk3 := bstream.TestBlockWithLIBNum("00000003", "00000002", 2)
	blk4 := bstream.TestBlockWithLIBNum("00000004", "00000003", 2)
	filenames := []string{
		bstream.BlockFileName(blk3),
		bstream.BlockFileNameWithSuffix(blk3, "otherreader"),
		bstream.BlockFileName(blk4),
	}

	out, err := fh.unknownOneBlockFiles(filenames, 0)
	require.NoError(t, err)
	assert.Equal(t, []string{filenames[0], filenames[2]}, out)

	out, err = fh.unknownOneBlockFiles(filenames, 4)
	require.NoError(t, err)
	assert.Equal(t, []string{filenames[2]}, out)
}
