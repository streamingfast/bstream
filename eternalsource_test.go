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

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/stretchr/testify/assert"
)

func TestEternalSource(t *testing.T) {
	doneCount := 0
	done := HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		if blk.Id == "00000003a" {
			return fmt.Errorf("failing block")
		}
		doneCount++
		return nil
	})

	srcFactory := NewTestSourceFactory()
	sf := SourceFromRefFactory(func(startRef BlockRef, h Handler) Source {
		return srcFactory.NewSourceFromRef(startRef, h)
	})

	eternalRestartWaitTime = 0
	s := NewEternalSource(sf, done)

	go s.Run()

	src := <-srcFactory.Created
	assert.NoError(t, src.Push(TestBlock("00000001a", "00000000a"), nil))
	assert.NoError(t, src.Push(TestBlock("00000002a", "00000001a"), nil))
	assert.Error(t, src.Push(TestBlock("00000003a", "00000002a"), nil))
	assert.Equal(t, 2, doneCount)
	src.Shutdown(nil)

	src = <-srcFactory.Created
	assert.Equal(t, "00000002a", src.StartBlockID)
	assert.NoError(t, src.Push(TestBlock("00000003b", "00000002a"), nil))
	assert.Equal(t, 3, doneCount)

	s.Shutdown(nil)
}

func TestDelegatingEternalSource(t *testing.T) {
	doneCount := 0
	done := HandlerFunc(func(blk *pbbstream.Block, obj interface{}) error {
		if blk.Id == "00000003a" {
			return fmt.Errorf("failing block")
		}
		doneCount++
		return nil
	})

	srcFactory := NewTestSourceFactory()
	sf := SourceFromRefFactory(func(startRef BlockRef, h Handler) Source {
		return srcFactory.NewSourceFromRef(startRef, h)
	})

	s := NewDelegatingEternalSource(sf, func() (BlockRef, error) { return bRef("00000001a"), nil }, done)
	s.restartDelay = 0

	go s.Run()

	src := <-srcFactory.Created
	assert.NoError(t, src.Push(TestBlock("00000001a", "00000000a"), nil))
	assert.NoError(t, src.Push(TestBlock("00000002a", "00000001a"), nil))
	assert.Error(t, src.Push(TestBlock("00000003a", "00000002a"), nil))
	assert.Equal(t, 2, doneCount)
	src.Shutdown(nil)

	src = <-srcFactory.Created
	assert.Equal(t, "00000001a", src.StartBlockID)
	assert.NoError(t, src.Push(TestBlock("00000002a", "00000001a"), nil))
	assert.NoError(t, src.Push(TestBlock("00000003b", "00000002a"), nil))
	assert.Equal(t, 4, doneCount)

	s.Shutdown(nil)
}
