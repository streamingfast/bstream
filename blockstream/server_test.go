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
	"fmt"
	"testing"

	"github.com/streamingfast/bstream"
	dgrpcserver "github.com/streamingfast/dgrpc/server"
	dgrpcstandard "github.com/streamingfast/dgrpc/server/standard"
	"github.com/stretchr/testify/require"
)

func testCreateGRPCServer() dgrpcserver.Server {
	return dgrpcstandard.NewServer(dgrpcserver.NewOptions())
}

func TestBlockServerSubscribe(t *testing.T) {
	s := NewBufferedServer(testCreateGRPCServer(), 2)
	s.PushBlock(bstream.TestBlock("00000002a", "00000001a"))
	s.PushBlock(bstream.TestBlock("00000003a", "00000002a"))
	sub := s.subscribe(2, "test")
	require.Len(t, s.subscriptions, 1)
	require.Len(t, sub.incomingBlock, 2)
	s.unsubscribe(sub)
	require.Len(t, s.subscriptions, 0)
}

func TestBlockServerSubscriptionChannelSize(t *testing.T) {
	s := NewUnmanagedServer()
	sub := s.subscribe(0, "default")
	require.Equal(t, DefaultSubscriptionChannelSize, cap(sub.incomingBlock))

	s = NewUnmanagedServer(ServerOptionWithSubscriptionChannelSize(0))
	sub = s.subscribe(0, "ignored")
	require.Equal(t, DefaultSubscriptionChannelSize, cap(sub.incomingBlock))

	s = NewBufferedServer(testCreateGRPCServer(), 2, ServerOptionWithSubscriptionChannelSize(5))
	s.PushBlock(bstream.TestBlock("00000002a", "00000001a"))
	s.PushBlock(bstream.TestBlock("00000003a", "00000002a"))
	sub = s.subscribe(2, "burst")
	require.Equal(t, 7, cap(sub.incomingBlock))
}

func TestBlockServerSubscriptionClosedWhenFull(t *testing.T) {
	s := NewUnmanagedServer(ServerOptionWithSubscriptionChannelSize(3))
	sub := s.subscribe(0, "slow")

	for i := 0; i < 3; i++ {
		require.NoError(t, s.PushBlock(bstream.TestBlockWithNumbers(fmt.Sprintf("%08xa", i+2), fmt.Sprintf("%08xa", i+1), uint64(i+2), 0)))
	}
	require.False(t, sub.closed, "subscription should hold up to its capacity")

	require.NoError(t, s.PushBlock(bstream.TestBlockWithNumbers("00000005a", "00000004a", 5, 0)))
	require.True(t, sub.closed, "subscription should close once over capacity")
}
