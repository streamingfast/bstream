package forkable

import (
	"fmt"
	"testing"

	"github.com/dfuse-io/bstream"
)

func BenchmarkForkDB_ReversibleSegments10(b *testing.B) { benchmarkForkDB(10, b) }

func BenchmarkForkDB_ReversibleSegments100(b *testing.B) { benchmarkForkDB(100, b) }

func BenchmarkForkDB_ReversibleSegments1000(b *testing.B) { benchmarkForkDB(1000, b) }

func BenchmarkForkDB_ReversibleSegments10000(b *testing.B) { benchmarkForkDB(10000, b) }

func BenchmarkForkDB_ReversibleSegments100000(b *testing.B) { benchmarkForkDB(100000, b) }

func benchmarkForkDB(blockCount int, b *testing.B) {
	forkdb := NewForkDB(ForkDBWithLogger(zlog))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 1; i <= blockCount; i++ {
		currentRef := bstream.BlockRefFromID(fmt.Sprintf("%08daa", i))
		previousRef := bstream.BlockRefFromID(fmt.Sprintf("%08daa", i-1))
		if i == 1 {
			forkdb.InitLIB(currentRef)
		}

		forkdb.AddLink(currentRef, previousRef, nil)
	}

	upToBlockID := bstream.BlockRefFromID(fmt.Sprintf("%08daa", blockCount))

	for n := 0; n < b.N; n++ {
		forkdb.ReversibleSegment(upToBlockID)
	}
}
