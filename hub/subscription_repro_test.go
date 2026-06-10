package hub

import (
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func reproBlock(id string, num uint64, idx int32, last bool) *pbbstream.Block {
	return &pbbstream.Block{
		Id:           id,
		Number:       num,
		ParentId:     fmt.Sprintf("%04da", num-1),
		ParentNum:    num - 1,
		LibNum:       1,
		Timestamp:    timestamppb.New(time.Now()),
		PartialIndex: idx,
		LastPartial:  last,
	}
}

type stepObj struct{ step bstream.StepType }

func (s stepObj) Step() bstream.StepType               { return s.step }
func (s stepObj) FinalBlockHeight() uint64             { return 0 }
func (s stepObj) ReorgJunctionBlock() bstream.BlockRef { return nil }

var _ bstream.Stepable = stepObj{}

// reproduces the live flashblocks flow under subscriber lag: the channel holds
// several blocks; the closing "last partial" (StepNewPartial) of a block is
// followed in the channel by a stale partial version of the same block number.
// The skip-to-latest-version logic must not skip past the closing block.
func TestSubscriptionEatsLastPartial(t *testing.T) {
	var received []string
	handler := bstream.HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		received = append(received, fmt.Sprintf("%s %s(idx=%d,last=%v)", obj.(bstream.Stepable).Step(), blk.Id, blk.PartialIndex, blk.LastPartial))
		return nil
	})

	sub := NewSubscription(handler, 100, true)

	push := func(step bstream.StepType, blk *pbbstream.Block) {
		if err := sub.push(&bstream.PreprocessedBlock{Block: blk, Obj: stepObj{step}}); err != nil {
			t.Fatal(err)
		}
	}

	// subscriber is lagging: all of these are already buffered in the channel
	// when it starts draining (flashblocks of block 4, its closing last-partial,
	// one stale post-seal flashblock version, then flashblocks of block 5)
	push(bstream.StepPartial, reproBlock("0004p1", 4, 1, false))
	push(bstream.StepPartial, reproBlock("0004p2", 4, 2, false))
	push(bstream.StepNewPartial, reproBlock("0004a", 4, 3, true)) // closing of block 4
	push(bstream.StepPartial, reproBlock("0004p4", 4, 4, false))  // stale flashblock version, post-seal
	push(bstream.StepPartial, reproBlock("0005p1", 5, 1, false))
	push(bstream.StepNewPartial, reproBlock("0005a", 5, 2, true)) // closing of block 5
	push(bstream.StepPartial, reproBlock("0006p1", 6, 1, false))

	go func() {
		time.Sleep(500 * time.Millisecond)
		sub.Shutdown(nil)
	}()
	sub.Run()

	for _, r := range received {
		fmt.Println("  delivered:", r)
	}

	for _, r := range received {
		if r == "new,partial 0004a(idx=3,last=true)" {
			return // closing was delivered, all good
		}
	}
	t.Fatalf("closing last-partial block 0004a was never delivered to the subscriber")
}
