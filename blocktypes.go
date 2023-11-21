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

import pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

type incomingBlocksFile struct {
	baseNum        uint64
	filename       string // Base filename (%100 of block_num)
	filteredBlocks []uint64
	blocks         chan *PreprocessedBlock
}

// PassesFilter will allow blocks to pass if they are >= than the
// first block in filteredBlocks (or if there is no filtering)
func (i *incomingBlocksFile) PassesFilter(blockNum uint64) bool {
	if i.filteredBlocks == nil {
		return true
	}

	var found bool
	for {
		if len(i.filteredBlocks) == 0 {
			return found
		}
		if blockNum >= i.filteredBlocks[0] {
			i.filteredBlocks = i.filteredBlocks[1:]
			found = true
			continue // ensure that we remove all previous filteredBlocks
		}
		return found
	}
}

func newIncomingBlocksFile(baseBlockNum uint64, baseFileName string, filteredBlocks []uint64) *incomingBlocksFile {
	ibf := &incomingBlocksFile{
		baseNum:        baseBlockNum,
		filename:       baseFileName,
		blocks:         make(chan *PreprocessedBlock, 0),
		filteredBlocks: filteredBlocks,
	}
	return ibf
}

type PreprocessedBlock struct {
	Block *pbbstream.Block
	Obj   interface{}
}

func (p *PreprocessedBlock) ID() string {
	return p.Block.Id
}

func (p *PreprocessedBlock) Num() uint64 {
	return p.Block.Number
}

func (p *PreprocessedBlock) String() string {
	return blockRefAsAstring(p)
}
