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

type incomingOneBlockFiles struct {
	filenames []string
	blocks    chan *PreprocessedBlock
}

type incomingBlocksFile struct {
	filename       string // Base filename (%100 of block_num)
	filteredBlocks []uint64
	blocks         chan *PreprocessedBlock
}

type PreprocessedBlock struct {
	Block *Block
	Obj   interface{}
}

func (p *PreprocessedBlock) ID() string {
	return p.Block.ID()
}

func (p *PreprocessedBlock) Num() uint64 {
	return p.Block.Num()
}

func (p *PreprocessedBlock) String() string {
	return blockRefAsAstring(p)
}
