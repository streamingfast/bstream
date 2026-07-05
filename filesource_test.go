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
	"bytes"
	"fmt"
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testBlocks(in ...*pbbstream.Block) []byte {
	buf := &bytes.Buffer{}
	blockWriter, err := NewDBinBlockWriter(buf)
	if err != nil {
		panic(err)
	}

	for _, blk := range in {
		blockWriter.Write(blk)
	}
	return buf.Bytes()
}

func base(in int) string {
	return fmt.Sprintf("%010d", in)
}

func TestFileSource_Deadlock(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		TestBlockWithNumbers("1a", "00", 1, 0),
		TestBlockWithNumbers("2a", "1a", 2, 0),
		TestBlockWithNumbers("3a", "2a", 3, 0),
		TestBlockWithNumbers("4a", "3a", 4, 0),
	))

	lastProcessed := 0
	handler := HandlerFunc(func(blk *pbbstream.Block, obj any) error {
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

	require.Equal(t, fs.Err(), errDone)
	assert.Equal(t, 2, lastProcessed)
}

func TestFileSource_Race(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		TestBlockWithNumbers("1a", "00", 1, 0),
		TestBlockWithNumbers("2a", "1a", 2, 0),
		TestBlockWithNumbers("3a", "2a", 3, 0),
		TestBlockWithNumbers("4a", "3a", 4, 0),
	))

	lastProcessed := 0
	shutMeDown := make(chan any)
	handler := HandlerFunc(func(blk *pbbstream.Block, obj any) error {
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
	require.NoError(t, fs.Err())
	assert.Equal(t, 3, lastProcessed, "race condition in filesource Run() when shutting down")
}

func TestFileSource_Run(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		TestBlockWithNumbers("1a", "00", 1, 0),
		TestBlockWithNumbers("2a", "1a", 2, 0),
	))
	bs.SetFile(base(100), testBlocks(
		TestBlockWithNumbers("103a", "2a", 103, 0),
		TestBlockWithNumbers("104a", "103a", 104, 0),
	))

	expectedBlocks := []uint64{1, 2, 103, 104}
	preProcessCount := 0
	preprocessor := PreprocessFunc(func(blk *pbbstream.Block) (any, error) {
		preProcessCount++
		return blk.Id, nil
	})

	testDone := make(chan any)
	handlerCount := 0
	handler := HandlerFunc(func(blk *pbbstream.Block, obj any) error {
		zlog.Debug("test : received block", zap.Stringer("block_ref", blk.AsRef()))
		require.Equal(t, expectedBlocks[handlerCount], blk.Number)
		require.Equal(t, blk.Id, obj.(ObjectWrapper).WrappedObject())
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
		TestBlockWithNumbers("1a", "00", 1, 0),
		TestBlockWithNumbers("2a", "1a", 2, 0),
		TestBlockWithNumbers("3a", "2a", 3, 0),
	))
	bs.SetFile(base(100), testBlocks(
		TestBlockWithNumbers("104a", "3a", 104, 0),
	))

	preProcessCount := 0
	preprocessor := PreprocessFunc(func(blk *pbbstream.Block) (any, error) {
		preProcessCount++
		return blk.Id, nil
	})

	expectedBlocks := []*BasicBlockRef{
		{id: "3a", num: 3},
		{id: "104a", num: 104},
	}
	expectedSteps := []StepType{
		StepIrreversible,
		StepNewIrreversible,
	}
	testDone := make(chan any)
	handlerCount := 0
	handler := HandlerFunc(func(blk *pbbstream.Block, obj any) error {
		zlog.Debug("test : received block", zap.Stringer("block_ref", blk.AsRef()))
		require.Equal(t, expectedBlocks[handlerCount].Num(), blk.Number)
		require.Equal(t, expectedSteps[handlerCount], obj.(Cursorable).Cursor().Step)
		require.Equal(t, blk.Id, obj.(ObjectWrapper).WrappedObject())
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

func TestFileSource_Run_BundleSize1000(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		TestBlockWithNumbers("1a", "00", 1, 0),
		TestBlockWithNumbers("2a", "1a", 2, 0),
		TestBlockWithNumbers("998a", "2a", 998, 0),
		TestBlockWithNumbers("999a", "998a", 999, 0),
	))
	bs.SetFile(base(1000), testBlocks(
		TestBlockWithNumbers("1000a", "999a", 1000, 0),
		TestBlockWithNumbers("1001a", "1000a", 1001, 0),
	))

	expectedBlocks := []uint64{1, 2, 998, 999, 1000, 1001}

	testDone := make(chan any)
	handlerCount := 0
	handler := HandlerFunc(func(blk *pbbstream.Block, obj any) error {
		require.Equal(t, expectedBlocks[handlerCount], blk.Number)
		if handlerCount >= len(expectedBlocks)-1 {
			close(testDone)
		}
		handlerCount++
		return nil
	})

	fs := NewFileSource(bs, 1, handler, zlog, FileSourceWithBundleSize(1000))
	go fs.Run()

	select {
	case <-testDone:
		require.Equal(t, len(expectedBlocks), handlerCount)
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}
	fs.Shutdown(nil)
}

func TestFileSource_DefaultMergedBlocksBundleSize(t *testing.T) {
	prev := DefaultMergedBlocksBundleSize
	defer func() { DefaultMergedBlocksBundleSize = prev }()

	DefaultMergedBlocksBundleSize = 1000
	fs := NewFileSource(dstore.NewMockStore(nil), 0, nil, zlog)
	assert.Equal(t, uint64(1000), fs.bundleSize)

	fs = NewFileSource(dstore.NewMockStore(nil), 0, nil, zlog, FileSourceWithBundleSize(200))
	assert.Equal(t, uint64(200), fs.bundleSize, "explicit option wins over default")
}

// A file containing a block at or beyond baseNum+bundleSize means the store
// holds bigger files than the configured bundle size: fail loudly instead of
// streaming out-of-bundle blocks.
func TestFileSource_Run_BundleSizeSmallerThanFiles(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		TestBlockWithNumbers("1a", "00", 1, 0),
		TestBlockWithNumbers("150a", "1a", 150, 0), // beyond bundle [0,100)
	))

	handler := HandlerFunc(func(blk *pbbstream.Block, obj any) error { return nil })

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

	require.Error(t, fs.Err())
	require.Contains(t, fs.Err().Error(), "beyond the configured bundle size")
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

// TestFileSource_lookupBlockIndex_LiveFloor checks that index-skipping stops at
// the live buffer floor: instead of skipping non-matching blocks all the way up
// to LastIndexedBlock (which would prevent the joining source from re-joining the
// live source), the lookup stops as soon as a bundle may overlap the live buffer
// and signals noMoreIndex so that bundle is read entirely. (firehose-core #109)
func TestFileSource_lookupBlockIndex_LiveFloor(t *testing.T) {
	// No matching blocks anywhere; without a floor this would skip up to base 400.
	// Live buffer floor is 250, so the lookup must stop at the bundle [200,300).
	fs := &FileSource{
		blockIndexProvider:        &TestBlockIndexProvider{Blocks: nil, LastIndexedBlock: 399},
		bundleSize:                100,
		logger:                    zlog,
		timeBetweenProgressBlocks: 10 * time.Second,
		liveBlockFloorGetter:      func() uint64 { return 250 },
	}

	baseBlock, blocks, noMoreIndex := fs.lookupBlockIndex(100)
	assert.True(t, noMoreIndex, "must stop using the index at the live overlap")
	assert.Equal(t, uint64(200), baseBlock, "stops at the first bundle that can contain the floor block")
	assert.Nil(t, blocks, "nil => bundle is read entirely, so overlap blocks are emitted for the join")

	// A floor of 0 (live buffer not ready) keeps the previous behaviour.
	fs.blockIndexProvider = &TestBlockIndexProvider{Blocks: nil, LastIndexedBlock: 399}
	fs.liveBlockFloorGetter = func() uint64 { return 0 }
	baseBlock, _, noMoreIndex = fs.lookupBlockIndex(100)
	assert.True(t, noMoreIndex)
	assert.Equal(t, uint64(400), baseBlock, "floor 0 disables the early stop")
}
