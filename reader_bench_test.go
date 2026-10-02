package bstream

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const benchBundleSize = 100

// benchMergedBlocks returns a merged blocks file of benchBundleSize blocks carrying a
// random payload of payloadSize bytes each.
func benchMergedBlocks(tb testing.TB, payloadSize int) []byte {
	tb.Helper()

	random := rand.New(rand.NewSource(1))
	buf := &bytes.Buffer{}
	writer, err := NewDBinBlockWriter(buf)
	require.NoError(tb, err)

	for i := uint64(0); i < benchBundleSize; i++ {
		payload := make([]byte, payloadSize)
		random.Read(payload)

		require.NoError(tb, writer.Write(&pbbstream.Block{
			Number:    21000000 + i,
			Id:        fmt.Sprintf("%064x", 21000000+i),
			ParentId:  fmt.Sprintf("%064x", 21000000+i-1),
			ParentNum: 21000000 + i - 1,
			LibNum:    21000000 + i - 64,
			Timestamp: timestamppb.New(time.Unix(1700000000+int64(i)*12, 0)),
			Payload:   &anypb.Any{TypeUrl: "type.googleapis.com/sf.ethereum.type.v2.Block", Value: payload},
		}))
	}

	return buf.Bytes()
}

var benchPayloadSizes = []int{4 * 1024, 256 * 1024, 2 * 1024 * 1024}

func BenchmarkDBinBlockReader_Read(b *testing.B) {
	for _, size := range benchPayloadSizes {
		data := benchMergedBlocks(b, size)

		b.Run(fmt.Sprintf("payload=%dKiB", size/1024), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				reader, err := NewDBinBlockReader(bytes.NewReader(data))
				if err != nil {
					b.Fatal(err)
				}

				count := 0
				for {
					_, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					count++
				}
				if count != benchBundleSize {
					b.Fatalf("expected %d blocks, got %d", benchBundleSize, count)
				}
			}
		})
	}
}
