// Copyright 2022 streamingfast Platform Inc.
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

	"github.com/stretchr/testify/assert"
)

func TestIncomingBlockFile_PassesFilter(t *testing.T) {
	tests := []struct {
		blockNum             uint64
		baseNum              uint64
		filteredBlocks       []uint64
		expectPass           bool
		expectFilteredBlocks []uint64
	}{
		{
			blockNum:             22,
			filteredBlocks:       []uint64{23},
			baseNum:              0,
			expectPass:           false,
			expectFilteredBlocks: []uint64{},
		},
		{
			blockNum:             22,
			filteredBlocks:       []uint64{22},
			baseNum:              0,
			expectPass:           true,
			expectFilteredBlocks: []uint64{},
		},
		{
			blockNum:             23,
			filteredBlocks:       []uint64{22, 50, 99},
			baseNum:              0,
			expectPass:           true,
			expectFilteredBlocks: []uint64{50, 99},
		},
		{
			blockNum:             23,
			filteredBlocks:       []uint64{11, 22, 50, 99},
			baseNum:              0,
			expectPass:           true,
			expectFilteredBlocks: []uint64{50, 99},
		},
		{
			blockNum:             99,
			filteredBlocks:       []uint64{11, 22, 50, 98},
			baseNum:              0,
			expectPass:           true,
			expectFilteredBlocks: []uint64{},
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			ibf := &incomingBlocksFile{
				baseNum:        test.baseNum,
				filteredBlocks: test.filteredBlocks,
			}
			pass := ibf.PassesFilter(test.blockNum)
			assert.Equal(t, test.expectPass, pass)
		})
	}

}
