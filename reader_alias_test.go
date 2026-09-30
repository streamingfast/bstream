package bstream

import (
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
