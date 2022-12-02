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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/test-go/testify/require"
)

var errTestMock = errors.New("test failure")

func testHandler(failAt uint64) (HandlerFunc, chan *PreprocessedBlock) {
	out := make(chan *PreprocessedBlock, 100)
	return func(blk *Block, obj interface{}) error {
		if blk.Number == failAt {
			return errTestMock
		}
		out <- &PreprocessedBlock{
			Block: blk,
			Obj:   obj,
		}
		return nil
	}, out
}

func TestJoiningSource_vanilla(t *testing.T) {
	joiningBlock := uint64(4)
	failingBlock := uint64(5)

	fileSF := NewTestSourceFactory()
	liveSF := NewTestSourceFactory()

	var liveSrc *TestSource

	handler, out := testHandler(failingBlock)
	liveSF.FromBlockNumFunc = func(num uint64, h Handler) Source {
		if num == joiningBlock {
			src := NewTestSource(h)
			liveSrc = src
			return src
		}
		return nil
	}

	joiningSource := NewJoiningSource(fileSF, liveSF, handler, 2, nil, false, zlog)
	go joiningSource.Run()

	fileSrc := <-fileSF.Created
	<-fileSrc.running // test fixture ready to push blocks
	assert.Equal(t, uint64(2), fileSrc.StartBlockNum)

	require.NoError(t, fileSrc.Push(TestBlock("00000002a", "00000001a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000003a", "00000002a"), nil))
	require.EqualError(t, fileSrc.Push(TestBlock("00000004a", "00000003a"), nil),
		stopSourceOnJoin.Error())

	<-fileSrc.Terminated() // previous error causes termination
	assert.Equal(t, 2, len(out))

	require.NotNil(t, liveSrc, "we should have joined to live source")

	require.NoError(t, liveSrc.Push(TestBlock("00000004a", "00000003a"), nil))
	assert.Equal(t, 3, len(out))

	require.EqualError(t, liveSrc.Push(TestBlock("00000005a", "00000004a"), nil),
		errTestMock.Error())

	<-liveSrc.Terminated()
	<-joiningSource.Terminated()
}

func TestJoiningSource_through_cursor(t *testing.T) {
	joiningBlock := uint64(6)
	failingBlock := uint64(9999)
	fileSF := NewTestSourceFactory()
	liveSF := NewTestSourceFactory()
	cursor := &Cursor{
		Step:      StepNew,
		Block:     NewBlockRefFromID("00000005"),
		HeadBlock: NewBlockRefFromID("00000005"),
		LIB:       NewBlockRefFromID("00000003"),
	}
	startBlock := uint64(3)

	var liveSrc *TestSource
	handler, out := testHandler(failingBlock)
	liveSF.ThroughCursorFunc = func(start uint64, cursor *Cursor, h Handler) Source {
		if start == joiningBlock {
			src := NewTestSource(h)
			src.Cursor = cursor
			src.StartBlockNum = start
			src.PassThroughCursor = true
			liveSrc = src
			return src
		}
		return nil
	}

	joiningSource := NewJoiningSource(fileSF, liveSF, handler, startBlock, cursor, true, zlog)
	go joiningSource.Run()

	fileSrc := <-fileSF.Created
	<-fileSrc.running // test fixture ready to push blocks
	assert.Equal(t, uint64(3), fileSrc.StartBlockNum)
	assert.Equal(t, cursor, fileSrc.Cursor)
	assert.True(t, fileSrc.PassThroughCursor)

	require.NoError(t, fileSrc.Push(TestBlock("00000003a", "00000002a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000004a", "00000003a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000005a", "00000004a"), nil))
	require.EqualError(t, fileSrc.Push(TestBlock("00000006a", "00000005a"), nil), stopSourceOnJoin.Error())

	<-fileSrc.Terminated()       // previous error causes termination
	assert.Equal(t, 3, len(out)) // 3, 4, 5 (6 will be sent by live)

	require.NotNil(t, liveSrc, "we should have joined to live source")
	assert.Equal(t, uint64(6), liveSrc.StartBlockNum)
	assert.Equal(t, cursor, liveSrc.Cursor)
	assert.True(t, liveSrc.PassThroughCursor)

	require.NoError(t, liveSrc.Push(TestBlock("00000006a", "00000005a"), nil))
	assert.Equal(t, 4, len(out))
}

func TestJoiningSource_skip_file_source(t *testing.T) {
	var fileSF ForkableSourceFactory //not used
	liveSF := NewTestSourceFactory()

	handler, out := testHandler(0)

	joiningSource := NewJoiningSource(fileSF, liveSF, handler, 2, nil, false, zlog)
	go joiningSource.Run()

	liveSrc := <-liveSF.Created
	<-liveSrc.running

	assert.Equal(t, uint64(2), liveSrc.StartBlockNum)

	require.NoError(t, liveSrc.Push(TestBlock("00000002a", "00000001a"), nil))
	require.NoError(t, liveSrc.Push(TestBlock("00000003a", "00000002a"), nil))

	assert.Len(t, out, 2)

	joiningSource.Shutdown(errTestMock)
	<-liveSrc.Terminated()       // previous error causes termination
	<-joiningSource.Terminated() // previous error causes termination
}

func TestJoiningSource_lowerLimitBackoff(t *testing.T) {
	fileSF := NewTestSourceFactory()
	liveSF := NewTestSourceFactory()

	liveSF.LowestBlkNum = 8
	joiningBlock := uint64(9)

	var liveSrc *TestSource
	liveSourceFactoryCalls := 0

	handler, out := testHandler(0)
	liveSF.FromBlockNumFunc = func(num uint64, h Handler) Source {
		liveSourceFactoryCalls++
		if num == joiningBlock {
			src := NewTestSource(h)
			liveSrc = src
			return src
		}
		return nil
	}

	joiningSource := NewJoiningSource(fileSF, liveSF, handler, 1, nil, false, zlog)
	go joiningSource.Run()

	fileSrc := <-fileSF.Created
	<-fileSrc.running
	assert.Equal(t, uint64(1), fileSrc.StartBlockNum)

	require.NoError(t, fileSrc.Push(TestBlock("00000001a", "00000000a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000002a", "00000001a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000003a", "00000002a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000004a", "00000003a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000005a", "00000004a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000006a", "00000005a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000007a", "00000006a"), nil))
	require.NoError(t, fileSrc.Push(TestBlock("00000008a", "00000007a"), nil))

	require.EqualError(t, fileSrc.Push(TestBlock("00000009a", "00000008a"), nil),
		stopSourceOnJoin.Error())

	<-fileSrc.Terminated() // previous error causes termination
	assert.Equal(t, 8, len(out))

	require.NotNil(t, liveSrc, "we should have joined to live source")
	require.NoError(t, liveSrc.Push(TestBlock("00000009a", "00000008a"), nil))
	assert.Equal(t, 9, len(out))

	assert.Equal(t, 3, liveSourceFactoryCalls)

}
