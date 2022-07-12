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

	leftSF := NewTestSourceFactory()
	rightSF := NewTestSourceFactory()

	var rightSrc *TestSource

	handler, out := testHandler(failingBlock)
	rightSF.FromBlockNumFunc = func(num uint64, h Handler) Source {
		if num == joiningBlock {
			src := NewTestSource(h)
			rightSrc = src
			return src
		}
		return nil
	}

	joiningSource := NewJoiningSource(leftSF, rightSF, handler, 2, nil, zlog)
	go joiningSource.Run()

	leftSrc := <-leftSF.Created
	<-leftSrc.running // test fixture ready to push blocks
	assert.Equal(t, uint64(2), leftSrc.StartBlockNum)

	require.NoError(t, leftSrc.Push(TestBlock("00000002a", "00000001a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000003a", "00000002a"), nil))
	require.EqualError(t, leftSrc.Push(TestBlock("00000004a", "00000003a"), nil),
		stopSourceOnJoin.Error())

	<-leftSrc.Terminated() // previous error causes termination
	assert.Equal(t, 2, len(out))

	require.NotNil(t, rightSrc, "we should have joined to right source")

	require.NoError(t, rightSrc.Push(TestBlock("00000004a", "00000003a"), nil))
	assert.Equal(t, 3, len(out))

	require.EqualError(t, rightSrc.Push(TestBlock("00000005a", "00000004a"), nil),
		errTestMock.Error())

	<-rightSrc.Terminated()
	<-joiningSource.Terminated()
}

func TestJoiningSource_skip_left_source(t *testing.T) {
	var fileSF ForkableSourceFactory //not used
	rightSF := NewTestSourceFactory()

	handler, out := testHandler(0)

	joiningSource := NewJoiningSource(fileSF, rightSF, handler, 2, nil, zlog)
	go joiningSource.Run()

	rightSrc := <-rightSF.Created
	<-rightSrc.running

	assert.Equal(t, uint64(2), rightSrc.StartBlockNum)

	require.NoError(t, rightSrc.Push(TestBlock("00000002a", "00000001a"), nil))
	require.NoError(t, rightSrc.Push(TestBlock("00000003a", "00000002a"), nil))

	assert.Len(t, out, 2)

	joiningSource.Shutdown(errTestMock)
	<-rightSrc.Terminated()      // previous error causes termination
	<-joiningSource.Terminated() // previous error causes termination
}

func TestJoiningSource_lowerLimitBackoff(t *testing.T) {
	leftSF := NewTestSourceFactory()
	rightSF := NewTestSourceFactory()

	rightSF.LowestBlkNum = 8
	joiningBlock := uint64(9)

	var rightSrc *TestSource
	rightSourceFactoryCalls := 0

	handler, out := testHandler(0)
	rightSF.FromBlockNumFunc = func(num uint64, h Handler) Source {
		rightSourceFactoryCalls++
		if num == joiningBlock {
			src := NewTestSource(h)
			rightSrc = src
			return src
		}
		return nil
	}

	joiningSource := NewJoiningSource(leftSF, rightSF, handler, 1, nil, zlog)
	go joiningSource.Run()

	leftSrc := <-leftSF.Created
	<-leftSrc.running
	assert.Equal(t, uint64(1), leftSrc.StartBlockNum)

	require.NoError(t, leftSrc.Push(TestBlock("00000001a", "00000000a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000002a", "00000001a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000003a", "00000002a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000004a", "00000003a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000005a", "00000004a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000006a", "00000005a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000007a", "00000006a"), nil))
	require.NoError(t, leftSrc.Push(TestBlock("00000008a", "00000007a"), nil))

	require.EqualError(t, leftSrc.Push(TestBlock("00000009a", "00000008a"), nil),
		stopSourceOnJoin.Error())

	<-leftSrc.Terminated() // previous error causes termination
	assert.Equal(t, 8, len(out))

	require.NotNil(t, rightSrc, "we should have joined to right source")
	require.NoError(t, rightSrc.Push(TestBlock("00000009a", "00000008a"), nil))
	assert.Equal(t, 9, len(out))

	assert.Equal(t, 3, rightSourceFactoryCalls)

}
