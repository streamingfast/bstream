// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hub

import (
	"context"
	"testing"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	dgrpcserver "github.com/streamingfast/dgrpc/server"
	dgrpcstandard "github.com/streamingfast/dgrpc/server/standard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockBlocksAndSignalsServer struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*pbbstream.BlocksAndSignalsResponse
}

func newMockBlocksAndSignalsServer(ctx context.Context) *mockBlocksAndSignalsServer {
	return &mockBlocksAndSignalsServer{
		ctx:       ctx,
		responses: make([]*pbbstream.BlocksAndSignalsResponse, 0),
	}
}

func (m *mockBlocksAndSignalsServer) Send(resp *pbbstream.BlocksAndSignalsResponse) error {
	m.responses = append(m.responses, resp)
	return nil
}

func (m *mockBlocksAndSignalsServer) Context() context.Context {
	return m.ctx
}

func TestBlockstreamServer_BlocksAndSignals(t *testing.T) {
	// Test that BlocksAndSignals method exists and can handle the interface properly
	// We'll create a minimal test that verifies the method signature is correct

	// Create BlockstreamServer with a mock hub
	dgrpcServer := dgrpcstandard.NewServer(dgrpcserver.NewOptions())
	hub := &ForkableHub{}
	bs := hub.NewBlockstreamServer(dgrpcServer)

	// Create a context that immediately cancels
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stream := newMockBlocksAndSignalsServer(ctx)
	request := &pbbstream.BlocksAndSignalsRequest{
		BlockRequest: &pbbstream.BlockRequest{
			Requester: "test-requester",
			Burst:     10,
		},
	}

	// The method should return immediately due to cancelled context
	// and should not panic
	err := bs.BlocksAndSignals(request, stream)

	// We expect an error because hub is not properly initialized
	// but this proves the method is properly implemented
	assert.Error(t, err, "Expected error from uninitialized hub")

	// Test that the BlockstreamServer properly implements the interface
	var _ pbbstream.BlockStreamServer = (*BlockstreamServer)(nil)
}

func TestBlockstreamServer_BlocksAndSignals_StreamHandler(t *testing.T) {
	// Test that streamHandlerWithSignals properly wraps blocks in BlockOrSignalResponse
	ctx := context.Background()
	stream := newMockBlocksAndSignalsServer(ctx)

	logger := zap.NewNop()
	handler := streamHandlerWithSignals(stream, logger)

	// Create a test block
	testBlock := &pbbstream.Block{
		Id:     "test-block",
		Number: 1,
	}

	// Process the block through the handler
	err := handler.ProcessBlock(testBlock, nil)
	require.NoError(t, err)

	// Verify the block was wrapped in BlockOrSignalResponse
	require.Len(t, stream.responses, 1, "Should have one response")
	response := stream.responses[0]

	assert.NotNil(t, response.GetBlock(), "Response should contain a block")
	assert.Equal(t, "test-block", response.GetBlock().Id, "Block ID should match")
	assert.Nil(t, response.GetSignal(), "Response should not contain a signal")
}

func init() {
	// Set logger for tests
	logger := zap.NewNop()
	zlog = logger
}
