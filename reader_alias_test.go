package bstream

import (
	"bytes"
	"io"
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUnmarshalBlockAliasingPayload(t *testing.T) {
	marshal := func(blk *pbbstream.Block) []byte {
		data, err := proto.Marshal(blk)
		require.NoError(t, err)
		return data
	}
	field := func(num protowire.Number, value []byte) []byte {
		out := protowire.AppendTag(nil, num, protowire.BytesType)
		return protowire.AppendBytes(out, value)
	}
	anyBytes := func(typeURL string, value []byte) []byte {
		return append(field(anyTypeURLField, []byte(typeURL)), field(anyValueField, value)...)
	}
	concat := func(parts ...[]byte) (out []byte) {
		for _, part := range parts {
			out = append(out, part...)
		}
		return out
	}

	full := &pbbstream.Block{
		Number:       10,
		Id:           "0a",
		ParentId:     "09",
		ParentNum:    9,
		LibNum:       5,
		Timestamp:    timestamppb.New(time.Unix(1700000000, 1)),
		Payload:      &anypb.Any{TypeUrl: "type.googleapis.com/sf.ethereum.type.v2.Block", Value: []byte{1, 2, 3}},
		PartialIndex: 3,
		LastPartial:  true,
	}
	header := marshal(&pbbstream.Block{Number: 10, Id: "0a"})

	tests := []struct {
		name    string
		message []byte
	}{
		{"full block", marshal(full)},
		{"empty message", nil},
		{"no payload", header},
		{"empty payload", concat(header, field(blockPayloadField, nil))},
		{"payload without value", concat(header, field(blockPayloadField, anyBytes("type.googleapis.com/a", nil)))},
		{"legacy payload buffer", marshal(&pbbstream.Block{Number: 10, PayloadKind: pbbstream.Protocol_ETH, PayloadBuffer: []byte{4, 5}})},
		{"payload first", concat(field(blockPayloadField, anyBytes("a", []byte{1})), header)},
		{"payload twice merges", concat(
			field(blockPayloadField, anyBytes("a", []byte{1})),
			header,
			field(blockPayloadField, field(anyValueField, []byte{2})),
		)},
		{"unknown fields", concat(header, field(99, []byte{7}), field(blockPayloadField, anyBytes("a", []byte{1})), field(98, nil))},
		{"payload with wrong wire type", concat(header, protowire.AppendVarint(protowire.AppendTag(nil, blockPayloadField, protowire.VarintType), 1))},
		{"invalid UTF-8 id", concat(field(2, []byte{0xff}), field(blockPayloadField, anyBytes("a", nil)))},
		{"invalid UTF-8 type url", concat(header, field(blockPayloadField, anyBytes("\xff", nil)))},
		{"truncated", marshal(full)[:len(marshal(full))-1]},
		{"truncated payload", concat(header, field(blockPayloadField, anyBytes("a", []byte{1, 2}))[:6])},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expected := &pbbstream.Block{}
			expectedErr := proto.Unmarshal(test.message, expected)

			actual := &pbbstream.Block{}
			actualErr := unmarshalBlockAliasingPayload(test.message, actual)

			if expectedErr != nil {
				assert.Error(t, actualErr)
				return
			}
			require.NoError(t, actualErr)
			assert.True(t, proto.Equal(expected, actual), "expected %v, got %v", expected, actual)
		})
	}

	t.Run("payload aliases the message", func(t *testing.T) {
		message := marshal(full)
		blk := &pbbstream.Block{}
		require.NoError(t, unmarshalBlockAliasingPayload(message, blk))

		blk.Payload.Value[0] = 42
		assert.Contains(t, string(message), string([]byte{42, 2, 3}))
	})
}

// Read hands out payloads pointing into the buffer each message was read into, which is
// only safe as long as dbin.Reader.ReadMessage never reuses that buffer. If it starts to,
// the payloads of the blocks read first get overwritten by the next ones.
func TestDBinBlockReader_PayloadsSurviveNextReads(t *testing.T) {
	payloads := [][]byte{
		bytes.Repeat([]byte{0xaa}, 1024),
		bytes.Repeat([]byte{0xbb}, 1024),
		bytes.Repeat([]byte{0xcc}, 1024),
	}

	buf := &bytes.Buffer{}
	writer, err := NewDBinBlockWriter(buf)
	require.NoError(t, err)
	for i, payload := range payloads {
		require.NoError(t, writer.Write(&pbbstream.Block{
			Number:  uint64(i + 1),
			Id:      "00",
			Payload: &anypb.Any{TypeUrl: "type.googleapis.com/sf.test.Block", Value: payload},
		}))
	}

	reader, err := NewDBinBlockReader(buf)
	require.NoError(t, err)

	var blocks []*pbbstream.Block
	for {
		blk, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		blocks = append(blocks, blk)
	}

	require.Len(t, blocks, len(payloads))
	for i, blk := range blocks {
		assert.Equal(t, payloads[i], blk.Payload.Value, "payload of block %d changed after reading the next blocks", blk.Number)
	}
}
