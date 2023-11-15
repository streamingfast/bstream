package bstream

import (
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"testing"
)

func TestToProtocol(t *testing.T) {
	// this is a weird test... technically the anypb should be a
	// protocol block, but for the purpose of this test we re-used a pbstream.Block
	pbBlk := pbbstream.Block{Id: "1", Number: 1, PreviousId: "foo"}
	protoAny, err := anypb.New(&pbBlk)
	require.NoError(t, err)

	bstreamBlk := &Block{
		Payload: protoAny,
	}

	out := ToProtocol[*pbbstream.Block](bstreamBlk)
	assert.Equal(t, "1", out.Id)
	assert.Equal(t, uint64(1), out.Number)
	assert.Equal(t, "foo", out.PreviousId)
}
