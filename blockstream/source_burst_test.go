package blockstream

import (
	"context"
	"errors"
	"testing"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type burstRecordingClient struct {
	burst int64
}

func (c *burstRecordingClient) Blocks(ctx context.Context, in *pbbstream.BlockRequest, opts ...grpc.CallOption) (pbbstream.BlockStream_BlocksClient, error) {
	c.burst = in.Burst
	return nil, errors.New("request recorded")
}

func TestSourceBurst(t *testing.T) {
	noopHandler := bstream.HandlerFunc(func(*pbbstream.Block, any) error { return nil })

	t.Run("fixed burst", func(t *testing.T) {
		client := &burstRecordingClient{}
		s := NewSource(context.Background(), "", 2, noopHandler)

		require.Error(t, s.run(client))
		assert.Equal(t, int64(2), client.burst)
	})

	t.Run("burst func is called when the source runs", func(t *testing.T) {
		client := &burstRecordingClient{}
		burst := int64(0)
		s := NewSource(context.Background(), "", 2, noopHandler, WithBurstFunc(func() int64 { return burst }))
		burst = -1234

		require.Error(t, s.run(client))
		assert.Equal(t, int64(-1234), client.burst)
	})
}
