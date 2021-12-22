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
	"github.com/streamingfast/atm"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/caching"
	"github.com/streamingfast/dgrpc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"math"
	"testing"
)

func testCreateGRPCServer() *grpc.Server {
	return dgrpc.NewServer()
}

func TestBlockServerSubscribe(t *testing.T) {
	diskCache := atm.NewCache("/tmp", math.MaxInt, math.MaxInt, atm.NewFileIO())
	engine := caching.NewCacheEngine("test", diskCache)
	caching.Engine = engine

	s := NewBufferedServer(testCreateGRPCServer(), 2)
	s.PushBlock(bstream.TestBlock("00000002a", "00000001a"))
	s.PushBlock(bstream.TestBlock("00000003a", "00000002a"))
	sub := s.subscribe(2, "test")
	require.Len(t, s.subscriptions, 1)
	require.Len(t, sub.incomingBlock, 2)
	s.unsubscribe(sub)
	require.Len(t, s.subscriptions, 0)
}
