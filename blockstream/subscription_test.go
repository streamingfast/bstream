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
	"testing"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/bstream"
)

func NewTestSubscription(chanSize int) *subscription {
	return newSubscription(chanSize, zlog)
}

func TestNewSubscription(t *testing.T) {
	tests := []struct {
		name                   string
		pushedMessages         []*pbbstream.Block
		expectedMessages       []*pbbstream.Block
		subscriptionBufferSize int
	}{
		{
			name:                   "sunny path",
			subscriptionBufferSize: 3,
			pushedMessages: []*pbbstream.Block{
				bstream.TestBlock("00000003a", "00000002a"),
				bstream.TestBlock("00000001a", "00000000a"),
				bstream.TestBlock("00000002a", "00000001a"),
			},
			expectedMessages: []*pbbstream.Block{
				bstream.TestBlock("00000001a", "00000000a"),
				bstream.TestBlock("00000002a", "00000001a"),
				bstream.TestBlock("00000003a", "00000002a"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := NewTestSubscription(test.subscriptionBufferSize)

			for _, msg := range test.pushedMessages {
				s.Push(msg)
			}

			// FIXME: Consume all messages from subscription and validated pushed messages and expected messages match
			// go s.start()
			// assert.True(t, c.expectMessages(len(test.expectedMessages)))
			// assert.EqualValues(t, test.expectedMessages, c.messages)
		})
	}
}
