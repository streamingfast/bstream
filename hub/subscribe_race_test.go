package hub

import (
	"sync"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
)

// Sources created from the hub at the same instant must all get live blocks. This is
// what happens on a relayer when the other relayer goes down: every client of the dead
// one sends its block request to the survivor within a few milliseconds.
func TestForkableHub_ConcurrentSubscribe(t *testing.T) {
	const clients = 8
	const rounds = 200

	for round := range rounds {
		lsf := bstream.NewTestSourceFactory()
		store := dstore.NewMockStore(nil)
		fh := NewForkableHub(lsf.NewSource, 0, store)
		AddToMockStore(t, store,
			bstream.TestBlockWithLIBNum("00000003", "00000002", 2),
			bstream.TestBlockWithLIBNum("00000004", "00000003", 2),
			bstream.TestBlockWithLIBNum("00000005", "00000004", 2),
			bstream.TestBlockWithLIBNum("00000008", "00000005", 3),
		)
		go fh.Run()
		select {
		case <-fh.Ready:
		case <-time.After(time.Second):
			t.Fatal("hub not ready")
		}
		liveSource := <-lsf.Created

		gotLiveBlock := make([]chan struct{}, clients)
		sources := make([]bstream.Source, clients)

		var wg sync.WaitGroup
		start := make(chan struct{})
		for i := range clients {
			gotLiveBlock[i] = make(chan struct{}, 1)
			handler := bstream.HandlerFunc(func(blk *pbbstream.Block, _ any) error {
				if blk.Id == "00000009" {
					gotLiveBlock[i] <- struct{}{}
				}
				return nil
			})

			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				// as BlockstreamServer.Blocks does for each block request
				sources[i] = fh.SourceFromBlockNumWithForks(3, handler, true)
			}()
		}
		close(start)
		wg.Wait()

		for i, source := range sources {
			require.NotNil(t, source, "round %d: client %d", round, i)
			go source.Run()
		}

		require.NoError(t, liveSource.Push(bstream.TestBlockWithLIBNum("00000009", "00000008", 3), nil))

		for i := range clients {
			select {
			case <-gotLiveBlock[i]:
			case <-time.After(time.Second):
				t.Fatalf("round %d: client %d never got the live block, the hub has %d subscribers for %d clients",
					round, i, len(fh.currentSubscribers()), clients)
			}
		}
		fh.Shutdown(nil)
	}
}
