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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecentBlockGetter(t *testing.T) {
	sf := NewTestSourceFactory()

	rbc := NewRecentBlockGetter(3)
	_ = sf.NewSource(rbc) // we use the one below instead
	src := <-sf.Created
	go src.Run()
	<-src.running // test fixture ready to push blocks

	require.NoError(t, src.Push(TestBlockWithTimestamp("00000001a", "00000000a", time.Now().UTC().Add(-time.Second*2)), nil))
	require.False(t, src.IsTerminating())
	require.Equal(t, "00000001a", rbc.LatestBlock().Id)

	require.NoError(t, src.Push(TestBlockWithTimestamp("00000002a", "00000001a", time.Now().UTC().Add(-time.Second*1)), nil))
	require.False(t, src.IsTerminating())
	require.Equal(t, "00000002a", rbc.LatestBlock().Id)

	assert.Equal(t, 2, rbc.counter)
	assert.Error(t, src.Push(TestBlockWithTimestamp("00000003b", "00000000a", time.Now().UTC().Add(-time.Second*3)), nil)) //old block
	require.Equal(t, "00000002a", rbc.LatestBlock().Id)

}
