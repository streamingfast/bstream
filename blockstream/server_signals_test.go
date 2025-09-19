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

package blockstream

import (
	"context"
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
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

func TestServer_BlocksAndSignals(t *testing.T) {
	logger := zap.NewNop()
	server := NewUnmanagedServer(ServerOptionWithLogger(logger))

	// Create a test block
	testBlock := &pbbstream.Block{
		Id:     "test-block-1",
		Number: 100,
		LibNum: 99,
	}

	// Create a test signal
	testSignal := &pbbstream.Signal{
		Signal: &pbbstream.Signal_BlockCommitmentSignal{
			BlockCommitmentSignal: &pbbstream.BlockCommitmentSignal{
				BlockId:         "block100",
				BlockNumber:     "100",
				CommitmentLevel: 1,
			},
		},
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create mock stream
	stream := newMockBlocksAndSignalsServer(ctx)

	// Start the BlocksAndSignals handler in a goroutine
	request := &pbbstream.BlocksAndSignalsRequest{
		BlockRequest: &pbbstream.BlockRequest{
			Requester: "test-requester",
			Burst:     0,
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.BlocksAndSignals(request, stream)
	}()

	// Give the subscription time to set up
	time.Sleep(100 * time.Millisecond)

	// Push a block
	err := server.PushBlock(testBlock)
	require.NoError(t, err)

	// Give time for the block to be processed
	time.Sleep(100 * time.Millisecond)

	// Push a signal
	err = server.PushSignal(testSignal)
	require.NoError(t, err)

	// Give time for the signal to be processed
	time.Sleep(100 * time.Millisecond)

	// Cancel context to stop the handler
	cancel()

	// Wait for handler to finish
	select {
	case <-errCh:
		// Handler finished
	case <-time.After(1 * time.Second):
		t.Fatal("Handler did not finish in time")
	}

	// Verify we received both block and signal
	require.Len(t, stream.responses, 2, "Expected 2 responses (1 block, 1 signal)")

	// Check first response is a block
	blockResp := stream.responses[0]
	require.NotNil(t, blockResp.GetBlock(), "First response should be a block")
	assert.Equal(t, testBlock.Id, blockResp.GetBlock().Id)
	assert.Equal(t, testBlock.Number, blockResp.GetBlock().Number)

	// Check second response is a signal
	signalResp := stream.responses[1]
	require.NotNil(t, signalResp.GetSignal(), "Second response should be a signal")
	require.NotNil(t, signalResp.GetSignal().GetBlockCommitmentSignal(), "Signal should contain BlockCommitmentSignal")
	assert.Equal(t, testSignal.GetBlockCommitmentSignal().BlockId, signalResp.GetSignal().GetBlockCommitmentSignal().BlockId)
	assert.Equal(t, testSignal.GetBlockCommitmentSignal().BlockNumber, signalResp.GetSignal().GetBlockCommitmentSignal().BlockNumber)
	assert.Equal(t, testSignal.GetBlockCommitmentSignal().CommitmentLevel, signalResp.GetSignal().GetBlockCommitmentSignal().CommitmentLevel)
}

func TestServer_MixedSubscriptions(t *testing.T) {
	logger := zap.NewNop()
	server := NewUnmanagedServer(ServerOptionWithLogger(logger))

	// Create test data
	testBlock := &pbbstream.Block{
		Id:     "test-block-2",
		Number: 200,
		LibNum: 199,
	}

	testSignal := &pbbstream.Signal{
		Signal: &pbbstream.Signal_BlockCommitmentSignal{
			BlockCommitmentSignal: &pbbstream.BlockCommitmentSignal{
				BlockId:         "block200",
				BlockNumber:     "200",
				CommitmentLevel: 2,
			},
		},
	}

	// Create contexts
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	// Create mock streams
	signalStream := newMockBlocksAndSignalsServer(ctx1)
	regularStream := newMockBlocksServer(ctx2)

	// Start BlocksAndSignals subscription
	signalRequest := &pbbstream.BlocksAndSignalsRequest{
		BlockRequest: &pbbstream.BlockRequest{
			Requester: "signal-requester",
			Burst:     0,
		},
	}

	errCh1 := make(chan error, 1)
	go func() {
		errCh1 <- server.BlocksAndSignals(signalRequest, signalStream)
	}()

	// Start regular Blocks subscription
	regularRequest := &pbbstream.BlockRequest{
		Requester: "regular-requester",
		Burst:     0,
	}

	errCh2 := make(chan error, 1)
	go func() {
		errCh2 <- server.Blocks(regularRequest, regularStream)
	}()

	// Give subscriptions time to set up
	time.Sleep(100 * time.Millisecond)

	// Push a block - both should receive it
	err := server.PushBlock(testBlock)
	require.NoError(t, err)

	// Push a signal - only signal subscription should receive it
	err = server.PushSignal(testSignal)
	require.NoError(t, err)

	// Give time for processing
	time.Sleep(200 * time.Millisecond)

	// Cancel contexts
	cancel1()
	cancel2()

	// Wait for handlers to finish
	select {
	case <-errCh1:
	case <-time.After(1 * time.Second):
		t.Fatal("Signal handler did not finish in time")
	}

	select {
	case <-errCh2:
	case <-time.After(1 * time.Second):
		t.Fatal("Regular handler did not finish in time")
	}

	// Verify signal stream received both block and signal
	assert.Len(t, signalStream.responses, 2, "Signal stream should have 2 responses")
	assert.NotNil(t, signalStream.responses[0].GetBlock(), "First response should be a block")
	assert.NotNil(t, signalStream.responses[1].GetSignal(), "Second response should be a signal")

	// Verify regular stream only received block
	assert.Len(t, regularStream.blocks, 1, "Regular stream should have 1 block")
	assert.Equal(t, testBlock.Id, regularStream.blocks[0].Id)
}

type mockBlocksServer struct {
	grpc.ServerStream
	ctx    context.Context
	blocks []*pbbstream.Block
}

func newMockBlocksServer(ctx context.Context) *mockBlocksServer {
	return &mockBlocksServer{
		ctx:    ctx,
		blocks: make([]*pbbstream.Block, 0),
	}
}

func (m *mockBlocksServer) Send(block *pbbstream.Block) error {
	m.blocks = append(m.blocks, block)
	return nil
}

func (m *mockBlocksServer) Context() context.Context {
	return m.ctx
}

func TestServer_BufferedBlocksAndSignals(t *testing.T) {
	logger := zap.NewNop()
	server := NewUnmanagedServer(
		ServerOptionWithLogger(logger),
		ServerOptionWithBuffer(3),
	)

	// Pre-populate buffer with blocks
	for i := 1; i <= 3; i++ {
		block := &pbbstream.Block{
			Id:     string(rune('a' + i - 1)),
			Number: uint64(i),
			LibNum: uint64(i - 1),
		}
		err := server.PushBlock(block)
		require.NoError(t, err)
	}

	// Create context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Create mock stream
	stream := newMockBlocksAndSignalsServer(ctx)

	// Start BlocksAndSignals with burst
	request := &pbbstream.BlocksAndSignalsRequest{
		BlockRequest: &pbbstream.BlockRequest{
			Requester: "burst-requester",
			Burst:     2, // Request last 2 blocks from buffer
		},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.BlocksAndSignals(request, stream)
	}()

	// Give time for burst to be sent
	time.Sleep(200 * time.Millisecond)

	// Push a signal
	signal := &pbbstream.Signal{
		Signal: &pbbstream.Signal_BlockCommitmentSignal{
			BlockCommitmentSignal: &pbbstream.BlockCommitmentSignal{
				BlockId:         "block4",
				BlockNumber:     "4",
				CommitmentLevel: 1,
			},
		},
	}
	err := server.PushSignal(signal)
	require.NoError(t, err)

	// Give time for signal to be processed
	time.Sleep(100 * time.Millisecond)

	// Cancel to stop handler
	cancel()

	// Wait for handler to finish
	select {
	case <-errCh:
	case <-time.After(1 * time.Second):
		t.Fatal("Handler did not finish in time")
	}

	// Verify we received burst blocks and then the signal
	require.Len(t, stream.responses, 3, "Expected 3 responses (2 burst blocks + 1 signal)")

	// Check first two responses are blocks from burst
	for i := 0; i < 2; i++ {
		blockResp := stream.responses[i]
		require.NotNil(t, blockResp.GetBlock(), "Response %d should be a block", i)
		// Should be blocks 2 and 3 (last 2 from buffer)
		assert.Equal(t, uint64(i+2), blockResp.GetBlock().Number)
	}

	// Check third response is the signal
	signalResp := stream.responses[2]
	require.NotNil(t, signalResp.GetSignal(), "Third response should be a signal")
	require.NotNil(t, signalResp.GetSignal().GetBlockCommitmentSignal(), "Signal should contain BlockCommitmentSignal")
	assert.Equal(t, signal.GetBlockCommitmentSignal().BlockId, signalResp.GetSignal().GetBlockCommitmentSignal().BlockId)
}
