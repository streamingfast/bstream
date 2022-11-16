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

package bstream

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"go.uber.org/zap"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
)

func testBlocks(in ...interface{}) (out []byte) {
	var blks []ParsableTestBlock
	for i := 0; i < len(in); i += 4 {
		blks = append(blks, ParsableTestBlock{
			Number:     uint64(in[i].(int)),
			ID:         in[i+1].(string),
			PreviousID: in[i+2].(string),
			LIBNum:     uint64(in[i+3].(int)),
		})
	}

	for _, blk := range blks {
		b, err := json.Marshal(blk)
		if err != nil {
			panic(err)
		}
		out = append(out, b...)
		out = append(out, '\n')
	}
	return
}

func base(in int) string {
	return fmt.Sprintf("%010d", in)
}

func TestFileSource_Deadlock(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2a", "", 0,
		3, "3a", "", 0,
		4, "4a", "", 0,
	))

	lastProcessed := 0
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		if blk.Number == 3 {
			return errDone
		}
		lastProcessed = int(blk.Number)
		return nil
	})

	fs := NewFileSource(bs, 1, handler, zlog)

	testDone := make(chan struct{})
	go func() {
		fs.Run()
		close(testDone)
	}()
	select {
	case <-testDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}

	assert.Equal(t, 2, lastProcessed)
}

func TestFileSource_Race(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2a", "", 0,
		3, "3a", "", 0,
		4, "4a", "", 0,
	))

	lastProcessed := 0
	shutMeDown := make(chan interface{})
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		if blk.Number == 3 {
			close(shutMeDown)
			time.Sleep(time.Millisecond * 50)
		}
		lastProcessed = int(blk.Number)
		return nil
	})

	fs := NewFileSource(bs, 1, handler, zlog)

	go func() {
		<-shutMeDown
		fs.Shutdown(nil)
	}()

	fs.Run()
	assert.Equal(t, 3, lastProcessed, "race condition in filesource Run() when shutting down")
}

func TestFileSource_Run(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2a", "", 0,
	))
	bs.SetFile(base(100), testBlocks(
		103, "103a", "", 0,
		104, "104a", "", 0,
	))

	expectedBlocks := []uint64{1, 2, 103, 104}
	preProcessCount := 0
	preprocessor := PreprocessFunc(func(blk *Block) (interface{}, error) {
		preProcessCount++
		return blk.ID(), nil
	})

	testDone := make(chan interface{})
	handlerCount := 0
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		zlog.Debug("test : received block", zap.Stringer("block_ref", blk))
		require.Equal(t, expectedBlocks[handlerCount], blk.Number)
		require.Equal(t, blk.ID(), obj.(ObjectWrapper).WrappedObject())
		if handlerCount >= len(expectedBlocks)-1 {
			close(testDone)
		}
		handlerCount++
		return nil
	})

	fs := NewFileSource(bs, 1, handler, zlog, FileSourceWithConcurrentPreprocess(preprocessor, 2))
	go fs.Run()

	select {
	case <-testDone:
		require.GreaterOrEqual(t, preProcessCount, len(expectedBlocks)) // preprocessor is in parallel
		require.Equal(t, len(expectedBlocks), handlerCount)
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}
	fs.Shutdown(nil)
}

func TestFileSourceFromCursor(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2a", "", 0,
		3, "3a", "", 0,
	))
	bs.SetFile(base(100), testBlocks(
		104, "104a", "", 0,
	))

	preProcessCount := 0
	preprocessor := PreprocessFunc(func(blk *Block) (interface{}, error) {
		preProcessCount++
		return blk.ID(), nil
	})

	expectedBlocks := []*BasicBlockRef{
		{id: "3a", num: 3},
		{id: "104a", num: 104},
	}
	expectedSteps := []StepType{
		StepIrreversible,
		StepNewIrreversible,
	}
	testDone := make(chan interface{})
	handlerCount := 0
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		zlog.Debug("test : received block", zap.Stringer("block_ref", blk))
		require.Equal(t, expectedBlocks[handlerCount].Num(), blk.Num())
		require.Equal(t, expectedSteps[handlerCount], obj.(Cursorable).Cursor().Step)
		require.Equal(t, blk.ID(), obj.(ObjectWrapper).WrappedObject())
		if handlerCount >= len(expectedBlocks)-1 {
			close(testDone)
		}
		handlerCount++
		return nil
	})

	fs := NewFileSourceFromCursor(bs, nil, &Cursor{
		Step:      StepNewIrreversible,
		Block:     NewBlockRef("3a", 3),
		HeadBlock: NewBlockRef("3a", 3),
		LIB:       NewBlockRef("2a", 2),
	}, handler, zlog, FileSourceWithConcurrentPreprocess(preprocessor, 2))
	go fs.Run()

	select {
	case <-testDone:
		require.GreaterOrEqual(t, preProcessCount, len(expectedBlocks)) // preprocessor is in parallel
		require.Equal(t, len(expectedBlocks), handlerCount)
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}
	fs.Shutdown(nil)
}

func TestFileSource_lookupBlockIndex(t *testing.T) {
	tests := []struct {
		name                        string
		in                          uint64
		startBlockNum               uint64
		stopBlockNum                uint64
		indexProvider               BlockIndexProvider
		simulatePassedProgressDelay bool
		expectBaseBlock             uint64
		expectOutBLocks             []uint64
		expectNoMoreIndex           bool
	}{
		{
			name:          "start 0, no stop block with blocks of interest in base file",
			in:            0,
			startBlockNum: 0,
			indexProvider: &TestBlockIndexProvider{
				Blocks:           []uint64{3, 16, 38, 76},
				LastIndexedBlock: 399,
			},
			expectBaseBlock:   0,
			expectOutBLocks:   []uint64{0, 3, 16, 38, 76},
			expectNoMoreIndex: false,
		},
		{
			name:          "start and stop block in same file with blocks of interest in between",
			in:            0,
			startBlockNum: 5,
			stopBlockNum:  50,
			indexProvider: &TestBlockIndexProvider{
				Blocks:           []uint64{3, 16, 38, 76},
				LastIndexedBlock: 399,
			},
			expectBaseBlock:   0,
			expectOutBLocks:   []uint64{5, 16, 38, 50},
			expectNoMoreIndex: false,
		},
		{
			name:          "start 0, looking at next run with blocks of interest",
			in:            100,
			startBlockNum: 0,
			indexProvider: &TestBlockIndexProvider{
				Blocks:           []uint64{108, 145, 171, 198},
				LastIndexedBlock: 399,
			},
			expectBaseBlock:   100,
			expectOutBLocks:   []uint64{108, 145, 171, 198},
			expectNoMoreIndex: false,
		},
		{
			name: "no more blocks of interest, goes up to LastIndexedBlock",
			in:   100,
			indexProvider: &TestBlockIndexProvider{
				Blocks:           nil,
				LastIndexedBlock: 399,
			},
			expectBaseBlock:   400,
			expectOutBLocks:   nil,
			expectNoMoreIndex: true,
		},
		{
			name: "no blocks of interest but we simulate duration of timeBetweenProgressBlocks passed",
			in:   100,
			indexProvider: &TestBlockIndexProvider{
				Blocks:           nil,
				LastIndexedBlock: 399,
			},
			expectBaseBlock:             100,
			expectOutBLocks:             []uint64{100},
			expectNoMoreIndex:           false,
			simulatePassedProgressDelay: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			progDelay := time.Second * 10
			if test.simulatePassedProgressDelay {
				progDelay = 0
			}
			fs := &FileSource{
				startBlockNum:             test.startBlockNum,
				stopBlockNum:              test.stopBlockNum,
				blockIndexProvider:        test.indexProvider,
				bundleSize:                100,
				logger:                    zlog,
				timeBetweenProgressBlocks: progDelay,
			}
			baseBlock, blocks, noMoreIndex := fs.lookupBlockIndex(test.in)
			assert.Equal(t, test.expectNoMoreIndex, noMoreIndex)
			assert.Equal(t, test.expectBaseBlock, baseBlock)
			assert.Equal(t, test.expectOutBLocks, blocks)
		})
	}

}
