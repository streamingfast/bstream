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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoiningSource(t *testing.T) {
	fileSF := NewTestSourceFactory()
	liveSF := NewTestSourceFactory()

	// filesf could be seeded with filesf.FromNum() or whatever
	doneCount := 0
	done := HandlerFunc(func(blk *Block, obj interface{}) error {
		doneCount++
		return nil
	})

	joiningSource := NewJoiningSource(fileSF.NewSource, liveSF.NewSource, done)
	go joiningSource.Run()
	fileSrc := <-fileSF.Created
	liveSrc := <-liveSF.Created

	<-liveSrc.running // test fixture ready to push blocks
	<-fileSrc.running // test fixture ready to push blocks

	require.NoError(t, liveSrc.Push(TestBlock("00000001a", "00000000a"), nil))
	joiningSource.state.log(joiningSource)
	require.NoError(t, fileSrc.Push(TestBlock("00000001a", "00000000a"), nil))

	require.Error(t, fileSrc.Push(TestBlock("00000003a", "00000001a"), nil))
	require.True(t, joiningSource.livePassThru)
	require.True(t, fileSrc.IsTerminating())
	require.NoError(t, fileSrc.Err())

	require.NoError(t, liveSrc.Push(TestBlock("00000003a", "00000001a"), nil))
	require.Equal(t, 2, doneCount)
	require.NoError(t, liveSrc.Push(TestBlock("00000004a", "00000003a"), nil))
	require.Equal(t, 3, doneCount)
	require.False(t, joiningSource.IsTerminating(), "too much cascading failure")

	joiningSource.Shutdown(nil)

	<-joiningSource.Terminating()
	<-liveSrc.Terminating()
}

func TestShutdownFilesourceCascade(t *testing.T) {
	fileSF := NewTestSourceFactory()
	liveSF := NewTestSourceFactory()

	joiningSource := NewJoiningSource(fileSF.NewSource, liveSF.NewSource, nil)
	go joiningSource.Run()
	fileSrc := <-fileSF.Created
	liveSrc := <-liveSF.Created
	<-liveSrc.running // test fixture ready to push blocks
	<-fileSrc.running // test fixture ready to push blocks

	// note: the fileSrc will already be shutdown with 'nil' if we are live
	fileSrc.Shutdown(fmt.Errorf("failing"))
	<-fileSrc.Terminating()
	<-liveSrc.Terminating()
	<-joiningSource.Terminating()
	assert.Error(t, fileSrc.Err(), "failing")
	assert.Error(t, joiningSource.Err(), "file source failed: failing")
	assert.Error(t, liveSrc.Err(), "file source failed: failing")
}

func TestLivePreprocessed(t *testing.T) {
	js := &JoiningSource{
		Shutter:        shutter.New(),
		liveBuffer:     NewBuffer("joiningSource", zlog),
		state:          joinSourceState{},
		liveBufferSize: 2,
		logger:         zlog,
	}
	err := js.incomingFromLive(TestBlock("00000002a", "00000001a"), "non-nil-obj")
	require.NoError(t, err)

	seen := make(map[string]bool)
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		seen[obj.(string)] = true
		return nil
	})

	js.handler = handler
	err = js.processLiveBuffer(TestBlock("00000002a", "00000001a"))
	require.NoError(t, err)

	assert.Equal(t, map[string]bool{"non-nil-obj": true}, seen)
}

func TestLive_Wrapper(t *testing.T) {
	liveSF := NewTestSourceFactory()

	doneCount := 0
	done := HandlerFunc(func(blk *Block, obj interface{}) error {
		require.False(t, blk.IsCloned())
		doneCount++
		return nil
	})

	joiningSource := NewJoiningSource(nil, liveSF.NewSource, done)
	joiningSource.livePassThru = true
	JoiningLiveSourceWrapper(done)(joiningSource)

	go joiningSource.Run()

	liveSrc := <-liveSF.Created
	<-liveSrc.running // test fixture ready to push blocks

	require.NoError(t, liveSrc.Push(TestBlock("00000001a", "00000000a"), nil))
	require.Equal(t, 1, doneCount) // only 1a passes, 2a is shut down before

	joiningSource.Shutdown(nil)
	<-joiningSource.Terminating()
}

func TestLive_Wrapper_and_PreProcessor(t *testing.T) {
	liveSF := NewTestSourceFactory()

	doneCount := 0
	done := HandlerFunc(func(blk *Block, obj interface{}) error {
		require.False(t, blk.IsCloned())
		doneCount++
		return nil
	})

	preProcessorCount := 0
	preProcessorHandler := NewPreprocessor(func(blk *Block) (interface{}, error) {
		require.False(t, blk.IsCloned())
		preProcessorCount++
		return nil, nil
	}, done)

	joiningSource := NewJoiningSource(nil, liveSF.NewSource, preProcessorHandler)
	joiningSource.livePassThru = true
	JoiningLiveSourceWrapper(done)(joiningSource)

	go joiningSource.Run()

	liveSrc := <-liveSF.Created
	<-liveSrc.running // test fixture ready to push blocks

	require.NoError(t, liveSrc.Push(TestBlock("00000001a", "00000000a"), nil))
	require.Equal(t, 1, doneCount) // only 1a passes, 2a is shut down before
	require.Equal(t, 1, preProcessorCount)

	joiningSource.Shutdown(nil)
	<-joiningSource.Terminating()
}

func TestJoiningSourceWithTracker(t *testing.T) {
	fileSF := NewTestSourceFactory()
	liveSF := NewTestSourceFactory()

	// filesf could be seeded with filesf.FromNum() or whatever
	doneCount := 0
	done := HandlerFunc(func(blk *Block, obj interface{}) error {
		//fmt.Println("BLOCK", blk.ID(), blk.Num())
		doneCount++
		return nil
	})

	joiningSource := NewJoiningSource(fileSF.NewSource, liveSF.NewSource, done)
	joiningSource.tracker = NewTracker(50)
	joiningSource.tracker.AddGetter(FileSourceHeadTarget, joiningSource.LastFileBlockRefGetter)
	joiningSource.tracker.AddGetter(LiveSourceHeadTarget, func(ctx context.Context) (BlockRef, error) {
		return &BasicBlockRef{
			id:  "00000003a",
			num: 4,
		}, nil
	})
	joiningSource.trackerTimeout = 10 * time.Millisecond

	go joiningSource.Run()

	fileSrc := <-fileSF.Created
	<-fileSrc.running // test fixture ready to push blocks

	require.NoError(t, fileSrc.Push(TestBlock("00000001a", "00000000a"), nil))

	liveSrc := <-liveSF.Created
	<-liveSrc.running // test fixture ready to push blocks

	require.NoError(t, liveSrc.Push(TestBlock("00000001a", "00000000a"), nil))
	require.Error(t, fileSrc.Push(TestBlock("00000003a", "00000001a"), nil))
	require.True(t, joiningSource.livePassThru)
	require.True(t, fileSrc.IsTerminating())
	require.NoError(t, fileSrc.Err())
	require.Equal(t, 1, doneCount) // only 1a passes, 2a is shut down before

	joiningSource.Shutdown(nil)

	<-joiningSource.Terminating()
	<-liveSrc.Terminating()
}
