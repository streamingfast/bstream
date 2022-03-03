// Copyright 2019 dfuse Platform Inc.  //
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

package transform

import (
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/assert"
)

func p(in uint64) *uint64 {
	return &in
}

func Test_InitializeFromFirstBlock(t *testing.T) {

	cases := []struct {
		name                         string
		definedStartBlock            *uint64
		blk                          bstream.BlockRef
		protocolFirstStreamableBlock uint64
		indexSizes                   []uint64
		expectedFirstBlockSeen       uint64
		expectedBaseBlockNums        map[uint64]uint64
	}{
		{
			name:                         "first_streamable_2",
			definedStartBlock:            nil,
			blk:                          bstream.NewBlockRef("x", 2),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       2,
			expectedBaseBlockNums:        map[uint64]uint64{100: 0, 1000: 0},
		},
		{
			name:                         "first_streamable_102",
			definedStartBlock:            nil,
			blk:                          bstream.NewBlockRef("x", 102),
			protocolFirstStreamableBlock: 102,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       102,
			expectedBaseBlockNums:        map[uint64]uint64{100: 100, 1000: 0},
		},
		{
			name:                         "holes on 100",
			definedStartBlock:            p(100),
			blk:                          bstream.NewBlockRef("x", 102),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       100,
			expectedBaseBlockNums:        map[uint64]uint64{100: 100, 1000: 0},
		},
		{
			name:                         "holes on 200",
			definedStartBlock:            p(200),
			blk:                          bstream.NewBlockRef("x", 202),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       200,
			expectedBaseBlockNums:        map[uint64]uint64{100: 200, 1000: 0},
		},
		{
			name:                         "very big hole will still defined Start Block",
			definedStartBlock:            p(200),
			blk:                          bstream.NewBlockRef("x", 602),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       200,
			expectedBaseBlockNums:        map[uint64]uint64{100: 200, 1000: 0},
		},
		{
			name:                         "defined Start Block only affects aligned index sizes",
			definedStartBlock:            p(1200),
			blk:                          bstream.NewBlockRef("x", 1200),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       1200,
			expectedBaseBlockNums:        map[uint64]uint64{100: 1200, 1000: 0},
		},
		{
			name:                         "defined Start Block only affects aligned index sizes, positive validation",
			definedStartBlock:            p(2000),
			blk:                          bstream.NewBlockRef("x", 2001),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       2000,
			expectedBaseBlockNums:        map[uint64]uint64{100: 2000, 1000: 2000},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bstream.GetProtocolFirstStreamableBlock = c.protocolFirstStreamableBlock
			idxer := &IrreversibleBlocksIndexer{
				definedStartBlock: c.definedStartBlock,
				baseBlockNums:     toMap(c.indexSizes),
			}
			idxer.initializeFromFirstBlock(c.blk)
			assert.Equal(t, c.expectedBaseBlockNums, idxer.baseBlockNums)
			assert.Equal(t, c.expectedFirstBlockSeen, idxer.firstBlockSeen)
		})
	}

}
