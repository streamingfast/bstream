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

	zero := uint64(0)
	two := uint64(2)
	oneOTwo := uint64(102)
	hundred := uint64(100)
	twohundreds := uint64(200)
	twelveHundred := uint64(1200)
	twothousands := uint64(2000)
	cases := []struct {
		name                         string
		definedStartBlock            *uint64
		blk                          bstream.BlockRef
		protocolFirstStreamableBlock uint64
		indexSizes                   []uint64
		expectedFirstBlockSeen       *uint64
		expectedBaseBlockNums        map[uint64]*uint64
	}{
		{
			name:                         "first_streamable_2",
			definedStartBlock:            nil,
			blk:                          bstream.NewBlockRef("x", 2),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &two,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &zero, 1000: &zero},
		},
		{
			name:                         "first_streamable_102",
			definedStartBlock:            nil,
			blk:                          bstream.NewBlockRef("x", 102),
			protocolFirstStreamableBlock: 102,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &oneOTwo,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &hundred, 1000: &zero},
		},
		{
			name:                         "holes on 100",
			definedStartBlock:            p(100),
			blk:                          bstream.NewBlockRef("x", 102),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &hundred,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &hundred, 1000: nil},
		},
		{
			name:                         "holes on 200",
			definedStartBlock:            p(200),
			blk:                          bstream.NewBlockRef("x", 202),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &twohundreds,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &twohundreds, 1000: nil},
		},
		{
			name:                         "very big hole will still defined Start Block",
			definedStartBlock:            p(200),
			blk:                          bstream.NewBlockRef("x", 602),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &twohundreds,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &twohundreds, 1000: nil},
		},
		{
			name:                         "defined Start Block only affects aligned index sizes",
			definedStartBlock:            p(1200),
			blk:                          bstream.NewBlockRef("x", 1200),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &twelveHundred,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &twelveHundred, 1000: nil},
		},
		{
			name:                         "defined Start Block only affects aligned index sizes, positive validation",
			definedStartBlock:            p(2000),
			blk:                          bstream.NewBlockRef("x", 2001),
			protocolFirstStreamableBlock: 2,
			indexSizes:                   []uint64{100, 1000},
			expectedFirstBlockSeen:       &twothousands,
			expectedBaseBlockNums:        map[uint64]*uint64{100: &twothousands, 1000: &twothousands},
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
