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

// A no-partial subscriber must not receive intermediate partials, but must receive the
// closing "last partial" of each block as if it were a full block (partial markers
// cleared), so it never stalls waiting for a separate PartialIndex==0 block. The
// original blocks (shared with other subscribers and the forkdb) must not be mutated.
func TestSubscriptionNoPartialPromotesLastPartial(t *testing.T) {
	var received []*pbbstream.Block
	handler := bstream.HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		received = append(received, blk)
		return nil
	})

	sub := NewSubscription(handler, 100, false) // withPartial=false

	lastPartial4 := reproBlock("0004a", 4, 3, true)
	full5 := reproBlock("00000005", 5, 0, false) // regular full block

	push := func(step bstream.StepType, blk *pbbstream.Block) {
		if err := sub.push(&bstream.PreprocessedBlock{Block: blk, Obj: stepObj{step}}); err != nil {
			t.Fatal(err)
		}
	}

	push(bstream.StepPartial, reproBlock("0004p1", 4, 1, false)) // intermediate -> dropped
	push(bstream.StepPartial, reproBlock("0004p2", 4, 2, false)) // intermediate -> dropped
	push(bstream.StepNewPartial, lastPartial4)                   // closing -> delivered as full block
	push(bstream.StepNew, full5)                                 // full block -> delivered as-is

	go func() {
		time.Sleep(200 * time.Millisecond)
		sub.Shutdown(nil)
	}()
	sub.Run()

	if len(received) != 2 {
		var ids []string
		for _, b := range received {
			ids = append(ids, fmt.Sprintf("%s(idx=%d)", b.Id, b.PartialIndex))
		}
		t.Fatalf("expected 2 blocks delivered, got %d: %v", len(received), ids)
	}

	// block 4: the last-partial, delivered with partial markers cleared
	if received[0].Id != "0004a" || received[0].PartialIndex != 0 || received[0].LastPartial {
		t.Fatalf("last-partial not promoted to full block: %+v", received[0])
	}
	// block 5: full block untouched
	if received[1].Id != "00000005" || received[1].PartialIndex != 0 {
		t.Fatalf("full block altered: %+v", received[1])
	}

	// the shared original block must be untouched (thin copy, no mutation)
	if lastPartial4.PartialIndex != 3 || !lastPartial4.LastPartial {
		t.Fatalf("original last-partial block was mutated: idx=%d last=%v", lastPartial4.PartialIndex, lastPartial4.LastPartial)
	}
}
