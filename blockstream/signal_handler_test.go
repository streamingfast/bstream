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
	"errors"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testSignalHandler implements both Handler and SignalHandler
type testSignalHandler struct {
	blocksReceived  []string
	signalsReceived []*pbbstream.Signal
	blockError      error
	signalError     error
}

func (h *testSignalHandler) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	h.blocksReceived = append(h.blocksReceived, blk.Id)
	return h.blockError
}

func (h *testSignalHandler) ProcessSignal(signal *pbbstream.Signal) error {
	h.signalsReceived = append(h.signalsReceived, signal)
	return h.signalError
}

// testBlockOnlyHandler only implements Handler
type testBlockOnlyHandler struct {
	blocksReceived []string
}

func (h *testBlockOnlyHandler) ProcessBlock(blk *pbbstream.Block, obj interface{}) error {
	h.blocksReceived = append(h.blocksReceived, blk.Id)
	return nil
}

// mockMixedClient returns a mix of blocks and signals
type mockMixedClient struct {
	grpc.ClientStream
	responses []pbbstream.BlocksAndSignalsResponse
	index     int
}

func (c *mockMixedClient) Recv() (*pbbstream.BlocksAndSignalsResponse, error) {
	if c.index >= len(c.responses) {
		// Keep returning blocks indefinitely after the predefined responses
		return &pbbstream.BlocksAndSignalsResponse{
			Response: &pbbstream.BlocksAndSignalsResponse_Block{
				Block: &pbbstream.Block{
					Number:    uint64(1000 + c.index),
					Id:        "block_infinite",
					Timestamp: timestamppb.Now(),
				},
			},
		}, nil
	}
	resp := c.responses[c.index]
	c.index++
	return &resp, nil
}

type mockMixedBlockStreamClient struct{}

func (c *mockMixedBlockStreamClient) Blocks(ctx context.Context, in *pbbstream.BlockRequest, opts ...grpc.CallOption) (pbbstream.BlockStream_BlocksClient, error) {
	return &mockBlocksClient{}, nil
}

func (c *mockMixedBlockStreamClient) BlocksAndSignals(ctx context.Context, in *pbbstream.BlocksAndSignalsRequest, opts ...grpc.CallOption) (pbbstream.BlockStream_BlocksAndSignalsClient, error) {
	return &mockMixedClient{
		responses: []pbbstream.BlocksAndSignalsResponse{
			{
				Response: &pbbstream.BlocksAndSignalsResponse_Block{
					Block: &pbbstream.Block{
						Number:    1,
						Id:        "block1",
						Timestamp: timestamppb.Now(),
					},
				},
			},
			{
				Response: &pbbstream.BlocksAndSignalsResponse_Signal{
					Signal: &pbbstream.Signal{
						Signal: &pbbstream.Signal_BlockCommitmentSignal{
							BlockCommitmentSignal: &pbbstream.BlockCommitmentSignal{
								BlockId:         "checkpoint",
								BlockNumber:     "100",
								CommitmentLevel: 1,
							},
						},
					},
				},
			},
			{
				Response: &pbbstream.BlocksAndSignalsResponse_Block{
					Block: &pbbstream.Block{
						Number:    2,
						Id:        "block2",
						Timestamp: timestamppb.Now(),
					},
				},
			},
			{
				Response: &pbbstream.BlocksAndSignalsResponse_Signal{
					Signal: &pbbstream.Signal{
						Signal: &pbbstream.Signal_BlockCommitmentSignal{
							BlockCommitmentSignal: &pbbstream.BlockCommitmentSignal{
								BlockId:         "flush",
								BlockNumber:     "200",
								CommitmentLevel: 2,
							},
						},
					},
				},
			},
			{
				Response: &pbbstream.BlocksAndSignalsResponse_Block{
					Block: &pbbstream.Block{
						Number:    3,
						Id:        "block3",
						Timestamp: timestamppb.Now(),
					},
				},
			},
		},
	}, nil
}

func TestSignalHandling(t *testing.T) {
	zlog, _ := zap.NewDevelopment()

	t.Run("handler_with_signal_support", func(t *testing.T) {
		handler := &testSignalHandler{}
		s := &Source{
			Shutter:        shutter.New(),
			ctx:            context.Background(),
			handler:        handler,
			preprocThreads: 1,
			logger:         zlog,
			requester:      "test",
		}

		// Run for a short time to process the predefined responses
		time.AfterFunc(50*time.Millisecond, func() {
			s.Shutdown(nil)
		})

		err := s.run(&mockMixedBlockStreamClient{})
		require.NoError(t, err)

		// Verify blocks were received
		assert.GreaterOrEqual(t, len(handler.blocksReceived), 3, "Should have received at least 3 blocks")
		assert.Contains(t, handler.blocksReceived, "block1")
		assert.Contains(t, handler.blocksReceived, "block2")
		assert.Contains(t, handler.blocksReceived, "block3")

		// Verify signals were received
		assert.Len(t, handler.signalsReceived, 2, "Should have received exactly 2 signals")
		assert.Equal(t, "checkpoint", handler.signalsReceived[0].GetBlockCommitmentSignal().BlockId)
		assert.Equal(t, "100", handler.signalsReceived[0].GetBlockCommitmentSignal().BlockNumber)
		assert.Equal(t, int32(1), handler.signalsReceived[0].GetBlockCommitmentSignal().CommitmentLevel)
		assert.Equal(t, "flush", handler.signalsReceived[1].GetBlockCommitmentSignal().BlockId)
		assert.Equal(t, "200", handler.signalsReceived[1].GetBlockCommitmentSignal().BlockNumber)
		assert.Equal(t, int32(2), handler.signalsReceived[1].GetBlockCommitmentSignal().CommitmentLevel)
	})

	t.Run("handler_without_signal_support", func(t *testing.T) {
		handler := &testBlockOnlyHandler{}
		s := &Source{
			Shutter:        shutter.New(),
			ctx:            context.Background(),
			handler:        handler,
			preprocThreads: 1,
			logger:         zlog,
			requester:      "test",
		}

		// Run for a short time to process the predefined responses
		time.AfterFunc(50*time.Millisecond, func() {
			s.Shutdown(nil)
		})

		err := s.run(&mockMixedBlockStreamClient{})
		require.NoError(t, err)

		// Verify blocks were received (signals should be ignored silently)
		assert.GreaterOrEqual(t, len(handler.blocksReceived), 3, "Should have received at least 3 blocks")
		assert.Contains(t, handler.blocksReceived, "block1")
		assert.Contains(t, handler.blocksReceived, "block2")
		assert.Contains(t, handler.blocksReceived, "block3")
	})

	t.Run("signal_error_does_not_shutdown", func(t *testing.T) {
		handler := &testSignalHandler{
			signalError: errors.New("signal processing error"),
		}
		s := &Source{
			Shutter:        shutter.New(),
			ctx:            context.Background(),
			handler:        handler,
			preprocThreads: 1,
			logger:         zlog,
			requester:      "test",
		}

		// Run for a short time to process the predefined responses
		time.AfterFunc(50*time.Millisecond, func() {
			s.Shutdown(nil)
		})

		err := s.run(&mockMixedBlockStreamClient{})
		require.NoError(t, err, "Signal errors should not cause shutdown")

		// Verify blocks were still processed despite signal errors
		assert.GreaterOrEqual(t, len(handler.blocksReceived), 3, "Should have received at least 3 blocks")
		assert.Len(t, handler.signalsReceived, 2, "Should have received 2 signals despite errors")
	})
}

func TestSignalHandlerInterface(t *testing.T) {
	t.Run("verify_interface_implementation", func(t *testing.T) {
		var handler bstream.Handler = &testSignalHandler{}

		// Verify it implements SignalHandler
		signalHandler, ok := handler.(bstream.SignalHandler)
		assert.True(t, ok, "testSignalHandler should implement SignalHandler")
		assert.NotNil(t, signalHandler)

		// Verify block-only handler doesn't implement SignalHandler
		var blockOnlyHandler bstream.Handler = &testBlockOnlyHandler{}
		_, ok = blockOnlyHandler.(bstream.SignalHandler)
		assert.False(t, ok, "testBlockOnlyHandler should not implement SignalHandler")
	})
}
