package forkable

import (
	"fmt"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func BenchmarkForkDB_AddLink(b *testing.B) {
	forkdb := NewForkDB(1, ForkDBWithLogger(zlog))

	require.True(b, b.N > 0, "at least 1 element supported in this benchmark")
	if b.N == 1 {
		b.ReportAllocs()
		b.ResetTimer()
		forkdb.AddLink(bRef("00000001aa"), "00000000aa", nil)
		return
	}

	var refs = make([]bstream.BlockRef, b.N+1)
	for n := 0; n <= b.N; n++ {
		refs[n] = bRef("00000001aa")
	}

	forkdb.AddLink(refs[1], refs[0].ID(), nil)
	forkdb.InitLIB(refs[1])

	b.ReportAllocs()
	b.ResetTimer()
	for n := 2; n <= b.N; n++ {
		forkdb.AddLink(refs[n], refs[n-1].ID(), nil)
	}
}

func BenchmarkForkDB_UsualCase(b *testing.B) {
	// Only use to get references to different elements of the ForkDB that going to be used
	tempDB, aaHead, _, cdHead, dcHead, eeHead := newUsualCaseForkDB()
	nextLIBRef := bRefInSegment(tempDB.libRef.Num()+12, "aa")

	tests := []struct {
		name           string
		tester         func(fdb *ForkDB)
		newDBOnEachRun bool
	}{
		{name: "has_new_irrreversible_segment", tester: func(fdb *ForkDB) { fdb.HasNewIrreversibleSegment(nextLIBRef) }},
		{name: "block_in_current_chain", tester: func(fdb *ForkDB) { fdb.BlockInCurrentChain(aaHead, nextLIBRef.Num()) }},
		{name: "move_lib", tester: func(fdb *ForkDB) { fdb.MoveLIB(nextLIBRef) }, newDBOnEachRun: true},
		{name: "reversible_segment", tester: func(fdb *ForkDB) { fdb.ReversibleSegment(nextLIBRef) }},
		{name: "chain_switch_on_fork_branch", tester: func(fdb *ForkDB) { fdb.ChainSwitchSegments(cdHead.ID(), prevRef(dcHead).ID()) }},
		{name: "chain_switch_two_same_length_forks", tester: func(fdb *ForkDB) { fdb.ChainSwitchSegments(aaHead.ID(), prevRef(eeHead).ID()) }},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()

			reusedFdb, _, _, _, _, _ := newUsualCaseForkDB()
			fdbs := make([]*ForkDB, b.N)
			for n := 0; n < b.N; n++ {
				if test.newDBOnEachRun {
					fdbs[n], _, _, _, _, _ = newUsualCaseForkDB()
				} else {
					fdbs[n] = reusedFdb
				}
			}

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				test.tester(fdbs[n])
			}
		})
	}
}

func BenchmarkForkDB_DegeneratedCases(b *testing.B) {
	tests := []struct {
		name           string
		tester         func(fdb *ForkDB, aaHead, eeHead bstream.BlockRef)
		newDBOnEachRun bool
	}{
		{name: "has_new_irrreversible_segment", tester: func(fdb *ForkDB, aaHead, _ bstream.BlockRef) { fdb.HasNewIrreversibleSegment(aaHead) }},
		{name: "block_in_current_chain", tester: func(fdb *ForkDB, aaHead, _ bstream.BlockRef) { fdb.BlockInCurrentChain(aaHead, fdb.libRef.Num()) }},
		{name: "move_lib", tester: func(fdb *ForkDB, aaHead, _ bstream.BlockRef) { fdb.MoveLIB(aaHead) }, newDBOnEachRun: true},
		{name: "reversible_segment", tester: func(fdb *ForkDB, aaHead, _ bstream.BlockRef) { fdb.ReversibleSegment(aaHead) }},
		{name: "chain_switch_segments", tester: func(fdb *ForkDB, aaHead, eeHead bstream.BlockRef) {
			fdb.ChainSwitchSegments(aaHead.ID(), prevRef(eeHead).ID())
		}},
	}

	for _, test := range tests {
		for i := 1; i <= 100000; i = i * 10 {
			b.Run(fmt.Sprintf("%d_blocks/%s", i, test.name), func(b *testing.B) {
				b.ReportAllocs()

				reusedFdb, aaHead, _, _, _, eeHead := newUsualCaseForkDB()
				aaHead = addSegment(reusedFdb, "aa", aaHead, i)

				fdbs := make([]*ForkDB, b.N)
				for n := 0; n < b.N; n++ {
					if test.newDBOnEachRun {
						fdb, aaHead, _, _, _, _ := newUsualCaseForkDB()
						aaHead = addSegment(fdb, "aa", aaHead, i)
						fdbs[n] = fdb
					} else {
						fdbs[n] = reusedFdb
					}
				}

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					test.tester(fdbs[n], aaHead, eeHead)
				}
			})
		}
	}
}

func BenchmarkForkDB_MoveLIB(b *testing.B) {
	for i := 1; i <= 100000; i = i * 10 {
		for j := 1; j <= i; j = j * 10 {
			b.Run(fmt.Sprintf("%d_blocks_move_%d", i, j), func(b *testing.B) {
				b.ReportAllocs()

				fdbs := make([]*ForkDB, b.N)
				for n := 0; n < b.N; n++ {
					fdbs[n], _ = newFilledLinear(i)
				}
				upToBlockRef := bRefInSegment(uint64(j), "aa")

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					fdbs[n].MoveLIB(upToBlockRef)
				}
			})
		}
	}
}

func BenchmarkForkDB_ReversibleSegments(b *testing.B) {
	for i := 1; i <= 100000; i = i * 10 {
		for j := 1; j <= i; j = j * 10 {
			b.Run(fmt.Sprintf("%d_blocks_reversible_%d", i, j), func(b *testing.B) {
				b.ReportAllocs()

				forkdb, _ := newFilledLinear(i)
				upToBlockRef := bRefInSegment(uint64(j), "aa")

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					forkdb.ReversibleSegment(upToBlockRef)
				}
			})
		}
	}
}

func BenchmarkForkDB_HasNewIrreversibleSegment(b *testing.B) {
	for i := 1; i <= 100000; i = i * 10 {
		for j := 1; j <= i; j = j * 10 {
			b.Run(fmt.Sprintf("%d_blocks_reversible_%d", i, j), func(b *testing.B) {
				b.ReportAllocs()

				forkdb, _ := newFilledLinear(i)
				upToBlockRef := bRefInSegment(uint64(j), "aa")

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					forkdb.HasNewIrreversibleSegment(upToBlockRef)
				}
			})
		}
	}
}

func BenchmarkForkDB_BlockInCurrentChain_FromHead(b *testing.B) {
	for i := 1; i <= 100000; i = i * 10 {
		for j := 1; j <= i; j = j * 10 {
			b.Run(fmt.Sprintf("%d_blocks_%d_from_head", i, j), func(b *testing.B) {
				b.ReportAllocs()
				forkdb, head := newFilledLinear(i)

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					forkdb.BlockInCurrentChain(head, uint64(j))
				}
			})
		}
	}
}

func BenchmarkForkDB_BlockInCurrentChain_FromFork(b *testing.B) {
	for i := 1; i <= 100000; i = i * 10 {
		for j := 1; j <= i; j = j * 10 {
			half := i / 2
			leftCount := half / 2
			rightCount := half / 2

			b.Run(fmt.Sprintf("%d_blocks(%d_left, %d_right)_from_fork_%d", half, leftCount, rightCount, j), func(b *testing.B) {
				b.ReportAllocs()

				forkdb, leftHead, _ := newFilledTwoForks(i, leftCount, rightCount)

				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					forkdb.BlockInCurrentChain(leftHead, uint64(j))
				}
			})
		}
	}
}

func BenchmarkForkDB_ChainSwitchSegments(b *testing.B) {
	for i := 1; i <= 100000; i = i * 10 {
		b.Run(fmt.Sprintf("%d_blocks_in_each_fork", i), func(b *testing.B) {
			b.ReportAllocs()

			forkdb, leftHead, rightHead := newFilledTwoForks(1, i, i)

			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				forkdb.ChainSwitchSegments(leftHead.ID(), prevRef(rightHead).ID())
			}
		})
	}
}

//
// ```
//                                +-> 352bb..356bb      +-> 364cd .. 366cd         +-> 370ee..376ee
//                                +                     +                          +
//    LIB +--> 350 Blocks +---> 351aa +--------------> 363aa +-----------------> 369aa
//                                                      +
//                                                      +-> 364dc .. 366dc
// ```
func newUsualCaseForkDB() (forkdb *ForkDB, aaHead, bbHead, cdHead, dcHead, eeHead bstream.BlockRef) {
	forkdb, aaHead = newFilledLinear(350)

	// We now have a forkdb with a linear 350 blocks segment (`aa`)
	// another 1 fork branch of 6 forked blocks (segment `ee`)/

	// We are going to add 4 forked blocks (segment `bb`) from here
	bbHead = addSegment(forkdb, "bb", aaHead, 4)

	// 12 blocks deeper, we add two times 2 forked blocks (segment `cd`, segment `dc`), and 6 blocks deeper
	aaHead = addSegment(forkdb, "aa", aaHead, 12)
	cdHead = addSegment(forkdb, "cd", aaHead, 2)
	dcHead = addSegment(forkdb, "dc", aaHead, 2)

	// 6 blocks deeper, we add another 1 fork branch of 6 forked blocks (segment `ee`)
	aaHead = addSegment(forkdb, "aa", aaHead, 6)
	eeHead = addSegment(forkdb, "ee", aaHead, 4)
	return
}

func newFilledTwoForks(nonForkCount, leftCount, rightCount int) (forkdb *ForkDB, leftHead, rightHead bstream.BlockRef) {
	var head bstream.BlockRef
	forkdb, head = newFilledLinear(nonForkCount)

	leftHead = addSegment(forkdb, "bb", head, leftCount)
	rightHead = addSegment(forkdb, "c", head, rightCount)

	if tracer.Enabled() {
		zlog.Debug("created two forks forkdb instance",
			zap.Stringer("left_head", leftHead),
			zap.Stringer("right_head", rightHead),
			zap.Stringer("lib", forkdb.libRef),
			zap.Int("link_count", len(forkdb.links)),
		)
	}

	return
}

func addSegment(forkdb *ForkDB, segment string, startAt bstream.BlockRef, count int) (head bstream.BlockRef) {
	head = startAt

	previousRef := startAt
	for i := 1; i <= count; i++ {
		head = bRefInSegment(startAt.Num()+uint64(i), segment)
		forkdb.AddLink(head, previousRef.ID(), nil)
		previousRef = head
	}
	return
}

func newFilledLinear(blockCount int) (forkdb *ForkDB, head bstream.BlockRef) {
	forkdb = NewForkDB(1, ForkDBWithLogger(zlog))

	for i := 1; i <= blockCount; i++ {
		currentRef := bRef(fmt.Sprintf("%08daa", i))
		previousRef := bRef(fmt.Sprintf("%08daa", i-1))

		forkdb.AddLink(currentRef, previousRef.ID(), nil)
		if i == 1 {
			forkdb.InitLIB(currentRef)
		}

		head = currentRef
	}

	if tracer.Enabled() {
		zlog.Debug("created a linear forkdb instance",
			zap.Stringer("head", head),
			zap.Int("link_count", len(forkdb.links)),
			zap.Stringer("lib", forkdb.libRef),
		)
	}

	return
}
