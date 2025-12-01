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
	"sync"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testHandler is a mock handler that records all ProcessBlock calls
type testHandler struct {
	mu    sync.Mutex
	calls []handlerCall
	err   error
}

type handlerCall struct {
	blockNum     uint64
	blockID      string
	step         bstream.StepType
	partialIndex int32
}

func (h *testHandler) ProcessBlock(block *pbbstream.Block, obj any) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.err != nil {
		return h.err
	}

	stepable := obj.(bstream.Stepable)
	call := handlerCall{
		blockNum:     block.Number,
		blockID:      block.Id,
		step:         stepable.Step(),
		partialIndex: block.PartialIndex,
	}
	h.calls = append(h.calls, call)
	return nil
}

func (h *testHandler) getCalls() []handlerCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]handlerCall(nil), h.calls...)
}

func (h *testHandler) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls = nil
	h.err = nil
}

// createPartialBlock creates a test block with a partial index
func createPartialBlock(id, previousID string, blockNum uint64, partialIndex int32) *bstream.PreprocessedBlock {
	block := bstream.TestBlock(id, previousID)
	block.Number = blockNum
	block.PartialIndex = partialIndex

	return &bstream.PreprocessedBlock{
		Block: block,
		Obj:   &testStepable{step: bstream.StepPartial},
	}
}

// createBlock creates a test block without partial index (regular block)
func createBlock(id, previousID string, blockNum uint64) *bstream.PreprocessedBlock {
	block := bstream.TestBlock(id, previousID)
	block.Number = blockNum
	block.PartialIndex = 0

	return &bstream.PreprocessedBlock{
		Block: block,
		Obj:   &testStepable{step: bstream.StepNew},
	}
}

// testStepable implements bstream.Stepable for testing
type testStepable struct {
	step bstream.StepType
}

func (t *testStepable) Step() bstream.StepType {
	return t.step
}

func (t *testStepable) FinalBlockHeight() uint64 {
	return 0
}

func (t *testStepable) ReorgJunctionBlock() bstream.BlockRef {
	return nil
}

func TestSubscription_Run_WithPartialBlocks(t *testing.T) {
	tests := []struct {
		name           string
		blocks         []*bstream.PreprocessedBlock
		expectedCalls  []handlerCall
		expectedNextID string // expected ID of block left in s.next after run completes
	}{
		{
			name: "sequence of partial blocks - expect call with latest partial",
			blocks: []*bstream.PreprocessedBlock{
				createPartialBlock("3partial1", "2", 3, 1),
				createPartialBlock("3partial2", "2", 3, 2),
				createPartialBlock("3partial3", "2", 3, 3),
			},
			expectedCalls: []handlerCall{
				{blockNum: 3, blockID: "3partial3", step: bstream.StepPartial, partialIndex: 3},
			},
			expectedNextID: "",
		},
		{
			name: "partial blocks followed by different block number",
			blocks: []*bstream.PreprocessedBlock{
				createPartialBlock("3partial1", "2", 3, 1),
				createPartialBlock("3partial2", "2", 3, 2),
				createBlock("4", "3", 4),
			},
			expectedCalls: []handlerCall{
				{blockNum: 3, blockID: "3partial2", step: bstream.StepPartial, partialIndex: 2},
				{blockNum: 4, blockID: "4", step: bstream.StepNew, partialIndex: 0},
			},
			expectedNextID: "",
		},
		{
			name: "single partial block",
			blocks: []*bstream.PreprocessedBlock{
				createPartialBlock("3partial1", "2", 3, 1),
			},
			expectedCalls: []handlerCall{
				{blockNum: 3, blockID: "3partial1", step: bstream.StepPartial, partialIndex: 1},
			},
			expectedNextID: "",
		},
		{
			name: "regular block without partials",
			blocks: []*bstream.PreprocessedBlock{
				createBlock("3", "2", 3),
			},
			expectedCalls: []handlerCall{
				{blockNum: 3, blockID: "3", step: bstream.StepNew, partialIndex: 0},
			},
			expectedNextID: "",
		},
		{
			name: "mixed sequence - partials then regular then partials",
			blocks: []*bstream.PreprocessedBlock{
				createPartialBlock("3partial1", "2", 3, 1),
				createPartialBlock("3partial2", "2", 3, 2),
				createBlock("4", "3", 4),
				createPartialBlock("5partial1", "4", 5, 1),
				createPartialBlock("5partial2", "4", 5, 2),
			},
			expectedCalls: []handlerCall{
				{blockNum: 3, blockID: "3partial2", step: bstream.StepPartial, partialIndex: 2},
				{blockNum: 4, blockID: "4", step: bstream.StepNew, partialIndex: 0},
				{blockNum: 5, blockID: "5partial2", step: bstream.StepPartial, partialIndex: 2},
			},
			expectedNextID: "",
		},
		{
			name:           "empty blocks channel",
			blocks:         []*bstream.PreprocessedBlock{},
			expectedCalls:  nil,
			expectedNextID: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &testHandler{}
			sub := NewSubscription(handler, 10)

			// Start run() in a goroutine
			var runErr error
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				runErr = sub.run()
			}()

			// Fill the channel with test blocks
			for _, block := range tt.blocks {
				err := sub.push(block)
				require.NoError(t, err)
			}

			// Give some time for blocks to be processed
			time.Sleep(10 * time.Millisecond)

			// Shutdown the subscription
			sub.Shutdown(nil)

			// Wait for run() to complete
			wg.Wait()
			require.NoError(t, runErr)

			// Verify the handler was called correctly
			actualCalls := handler.getCalls()
			assert.Equal(t, tt.expectedCalls, actualCalls)

			// Verify s.next state
			if tt.expectedNextID == "" {
				assert.Nil(t, sub.next)
			} else {
				require.NotNil(t, sub.next)
				assert.Equal(t, tt.expectedNextID, sub.next.Block.Id)
			}
		})
	}
}

func TestSubscription_Push(t *testing.T) {
	handler := &testHandler{}
	sub := NewSubscription(handler, 2) // small channel size

	// Should be able to push up to channel capacity
	block1 := createBlock("1", "0", 1)
	err := sub.push(block1)
	assert.NoError(t, err)

	block2 := createBlock("2", "1", 2)
	err = sub.push(block2)
	assert.NoError(t, err)

	// Should fail when channel is full
	block3 := createBlock("3", "2", 3)
	err = sub.push(block3)
	assert.Equal(t, ErrSubscriptionChannelFull, err)
}

func TestLookAhead(t *testing.T) {
	// Test with empty channel
	ch := make(chan *bstream.PreprocessedBlock, 2)
	result := lookAhead(ch)
	assert.Nil(t, result)

	// Test with block in channel
	block := createBlock("1", "0", 1)
	ch <- block
	result = lookAhead(ch)
	require.NotNil(t, result)
	assert.Equal(t, "1", result.Block.Id)

	// Channel should now be empty
	result = lookAhead(ch)
	assert.Nil(t, result)
}

func TestNewSubscription(t *testing.T) {
	handler := &testHandler{}
	chanSize := 5

	sub := NewSubscription(handler, chanSize)

	assert.NotNil(t, sub)
	assert.Equal(t, handler, sub.handler)
	assert.Equal(t, chanSize, cap(sub.blocks))
	assert.Nil(t, sub.next)
	assert.NotNil(t, sub.Shutter)
}
