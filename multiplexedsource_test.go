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
	"fmt"
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiplexedSource(t *testing.T) {
	sourceReconnectDelay = time.Duration(0)

	sfOne := NewTestSourceFactory()
	sfTwo := NewTestSourceFactory()

	doneCount := 0
	done := HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		doneCount++
		return nil
	})

	mplex := NewMultiplexedSource([]SourceFactory{sfOne.NewSource, sfTwo.NewSource}, done)
	go mplex.Run()
	srcOne := <-sfOne.Created
	srcTwo := <-sfTwo.Created

	<-srcOne.running // test fixture ready to push blocks
	<-srcTwo.running // test fixture ready to push blocks

	now := time.Now().UTC()

	require.NoError(t, srcOne.Push(TestBlockWithTimestamp("00000002a", "00000001a", now), nil))
	require.Equal(t, 1, doneCount)

	srcTwo.Shutdown(fmt.Errorf("test"))
	<-srcTwo.Terminating()

	srcTwo = <-sfTwo.Created
	require.NoError(t, srcTwo.Push(TestBlockWithTimestamp("00000004a", "00000003a", now), nil))

	require.Equal(t, 2, doneCount, "sending new block from reconnected source")

	mplex.Shutdown(nil)
	<-srcOne.Terminating()
	<-srcTwo.Terminating()
}

func TestMultiplexedSource_shutdownOnProcessError(t *testing.T) {
	sf := NewTestSourceFactory()
	done := HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		return fmt.Errorf("please leave")
	})

	mplex := NewMultiplexedSource([]SourceFactory{sf.NewSource}, done)
	go mplex.Run()

	src := <-sf.Created
	<-src.running // test fixture ready to push blocks

	assert.Error(t, src.Push(TestBlock("00000002a", "00000001a"), nil))
	select {
	case <-mplex.Terminating():
	case <-time.After(100 * time.Millisecond):
		t.Errorf("timed out waiting for multiplexedSource to shutdown")
	}

}

func TestMultiplexedSource_noShutdownOnSrcShutdown(t *testing.T) {
	sf := NewTestSourceFactory()
	done := HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		return fmt.Errorf("please leave")
	})

	mplex := NewMultiplexedSource([]SourceFactory{sf.NewSource}, done)
	go mplex.Run()
	src := <-sf.Created

	<-src.running // test fixture ready to push blocks
	src.Shutdown(fmt.Errorf("shutting down source"))
	<-src.Terminating()
	assert.False(t, mplex.IsTerminating(), "multiplexedSource should not go down on source shutdown")

}
