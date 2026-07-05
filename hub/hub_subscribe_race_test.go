package hub

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/require"
)

// TestForkableHub_ConcurrentSubscribe exercises concurrent subscriptions while
// blocks are being broadcast. All SourceFrom* paths call subscribe under the
// forkable *read* lock only, so two concurrent subscribers used to race on the
// h.subscribers slice (caught by -race) and could lose a subscription entirely.
func TestForkableHub_ConcurrentSubscribe(t *testing.T) {
	fh := &ForkableHub{
		Shutter:           shutter.New(),
		logger:            zlog,
		sourceChannelSize: 100,
	}
	fh.forkable = forkable.New(bstream.HandlerFunc(fh.broadcastBlock),
		forkable.HoldBlocksUntilLIB(),
		forkable.WithKeptFinalBlocks(100),
		forkable.WithFilters(bstream.StepsAllWithPartial),
	)

	// Seed the forkable so that SourceFromBlockNum can serve block 4.
	require.NoError(t, fh.forkable.ProcessBlock(bstream.TestBlockWithLIBNum("00000003", "00000002", 2), nil))
	require.NoError(t, fh.forkable.ProcessBlock(bstream.TestBlockWithLIBNum("00000004", "00000003", 3), nil))

	const subscriberCount = 20
	const lastBroadcastBlock = uint64(40)

	start := make(chan struct{})

	// Broadcaster: pushes blocks 5..40 through the forkable while subscribers register.
	broadcastDone := make(chan struct{})
	go func() {
		defer close(broadcastDone)
		<-start
		prev := "00000004"
		for num := uint64(5); num <= lastBroadcastBlock; num++ {
			id := fmt.Sprintf("%08x", num)
			if err := fh.forkable.ProcessBlock(bstream.TestBlockWithLIBNum(id, prev, 3), nil); err != nil {
				panic(err)
			}
			prev = id
		}
	}()

	type subscriber struct {
		source   bstream.Source
		received chan uint64
	}
	subs := make([]*subscriber, subscriberCount)

	var wg sync.WaitGroup
	for i := 0; i < subscriberCount; i++ {
		sub := &subscriber{received: make(chan uint64, 256)}
		subs[i] = sub
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			handler := bstream.HandlerFunc(func(blk *pbbstream.Block, _ any) error {
				sub.received <- blk.Number
				return nil
			})
			sub.source = fh.SourceFromBlockNum(4, handler)
		}()
	}

	close(start)
	wg.Wait()
	<-broadcastDone

	// Every subscription must exist and receive blocks up to the last one
	// broadcast after it registered. A lost subscription (dropped by a racing
	// append) would never receive it.
	for i, sub := range subs {
		require.NotNil(t, sub.source, "subscriber %d did not get a source", i)
		go sub.source.Run()

		var last uint64
		timeout := time.After(5 * time.Second)
	drain:
		for {
			select {
			case num := <-sub.received:
				if num > last {
					last = num
				}
				if last == lastBroadcastBlock {
					break drain
				}
			case <-timeout:
				t.Fatalf("subscriber %d timed out waiting for block %d, last received %d (lost subscription?)", i, lastBroadcastBlock, last)
			}
		}
		sub.source.Shutdown(nil)
	}
}
