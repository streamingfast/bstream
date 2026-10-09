package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/hub"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errReachedHead = errors.New("reached head")

func TestStream_StartBlockAgainstHubBuffer(t *testing.T) {
	const headNum = 220

	tests := []struct {
		name        string
		startBlock  int64
		missingNum  uint64
		expectFirst uint64
	}{
		{"below the hub buffer", 50, 0, 50},
		{"just below the hub buffer", 99, 0, 99},
		{"at the hub lowest block", 100, 0, 100},
		{"missing block inside the hub buffer", 210, 210, 211},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mergedStore := dstore.NewMockStore(nil)
			for base := uint64(0); base < 200; base += 100 {
				var blks []*pbbstream.Block
				for num := base; num < base+100; num++ {
					if num == 0 {
						continue
					}
					blks = append(blks, testBlock(num, num-1, num-1))
				}
				mergedStore.SetFile(fmt.Sprintf("%010d", base), encodeBlocks(t, blks...))
			}

			oneBlocksStore := dstore.NewMockStore(nil)
			parent := uint64(99)
			for num := uint64(100); num <= headNum; num++ {
				if num == test.missingNum {
					continue
				}
				blk := testBlock(num, parent, num-5)
				oneBlocksStore.SetFile(bstream.BlockFileName(blk), encodeBlocks(t, blk))
				parent = num
			}

			h := hub.NewForkableHub(bstream.NewTestSourceFactory().NewSource, 150, oneBlocksStore)
			go h.Run()
			defer h.Shutdown(nil)
			select {
			case <-h.Ready:
			case <-time.After(5 * time.Second):
				t.Fatal("hub not ready")
			}
			require.Equal(t, uint64(100), h.LowestBlockNum())

			var got []uint64
			handler := bstream.HandlerFunc(func(blk *pbbstream.Block, _ any) error {
				got = append(got, blk.Number)
				if blk.Number == headNum {
					return errReachedHead
				}
				return nil
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := New(dstore.NewMockStore(nil), mergedStore, h, test.startBlock, handler).Run(ctx)
			require.ErrorIs(t, err, errReachedHead)

			require.NotEmpty(t, got)
			assert.Equal(t, test.expectFirst, got[0])
			for i := 1; i < len(got); i++ {
				require.Equal(t, got[i-1]+1, got[i], "gap after block %d", got[i-1])
			}
		})
	}
}

func testBlock(num, parent, lib uint64) *pbbstream.Block {
	return bstream.TestBlockFromJSON(fmt.Sprintf(`{"id":"%08x","prev":"%08x","num":%d,"prevnum":%d,"libnum":%d}`, num, parent, num, parent, lib))
}

func encodeBlocks(t *testing.T, blks ...*pbbstream.Block) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	writer, err := bstream.NewDBinBlockWriter(buf)
	require.NoError(t, err)
	for _, blk := range blks {
		require.NoError(t, writer.Write(blk))
	}
	return buf.Bytes()
}
