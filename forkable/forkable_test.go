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

package forkable

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/dfuse-io/bstream"
	eos "github.com/eoscanada/eos-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForkable_ProcessBlock(t *testing.T) {
	cases := []struct {
		name                               string
		forkDB                             *ForkDB
		ensureAllBlocksTriggerLongestChain bool
		ensureBlockFlows                   bstream.BlockRef
		includeInitialLIB                  bool
		filterSteps                        StepType
		processBlocks                      []*bstream.Block
		undoErr                            error
		redoErr                            error
		startBlock                         uint64
		expectedResultCount                int
		expectedResult                     []*ForkableObject
		expectedError                      string
	}{
		{
			name:              "inclusive enabled",
			forkDB:            fdbLinked("00000003a"),
			startBlock:        3,
			includeInitialLIB: true,
			processBlocks: []*bstream.Block{
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000004a", "00000003a"), //StepNew 00000002a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepIrreversible,
					Obj:  "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000003a", "00000002a"), "00000003a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
			},
		},
		{
			name:       "inclusive disabled",
			forkDB:     fdbLinked("00000003a"),
			startBlock: 3,
			processBlocks: []*bstream.Block{
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000004a", "00000003a"), //StepNew 00000002a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
			},
		},
		{
			name:   "undos redos and skip",
			forkDB: fdbLinked("00000001a"),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"), //StepNew 00000002a
				bTestBlock("00000003a", "00000002a"), //StepNew 00000003a
				bTestBlock("00000003b", "00000002a"), //nothing
				bTestBlock("00000004b", "00000003b"), //StepUndo 00000003a, StepNew 00000003b, StepNew 00000004b
				bTestBlock("00000004a", "00000003a"), //nothing not longest chain
				bTestBlock("00000005a", "00000004a"), //StepUndo 00000004b, StepUndo 00000003b, StepRedo 00000003a, StepNew 00000004a
				bTestBlock("00000007a", "00000006a"), //nothing not longest chain
				bTestBlock("00000006a", "00000005a"), //nothing
				bTestBlock("00000008a", "00000007a"), //StepNew 00000007a, StepNew 00000008a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step:      StepUndo,
					Obj:       "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000003a", "00000002a"), "00000003a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000003b",
				},
				{
					Step: StepNew,
					Obj:  "00000004b",
				},
				{
					Step:      StepUndo,
					Obj:       "00000004b",
					StepCount: 2,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000004b", "00000003b"), "00000004b"},
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
				{
					Step:      StepUndo,
					Obj:       "00000003b",
					StepCount: 2,
					StepIndex: 1,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000004b", "00000003b"), "00000004b"},
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
				{
					Step:      StepRedo,
					Obj:       "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000003a", "00000002a"), "00000003a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000005a",
				},
				{
					Step: StepNew,
					Obj:  "00000006a",
				},
				{
					Step: StepNew,
					Obj:  "00000007a",
				},
				{
					Step: StepNew,
					Obj:  "00000008a",
				},
			},
		},
		{
			name:       "irreversible",
			forkDB:     fdbLinked("00000001a"),
			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"),              //StepNew 00000002a
				bTestBlockWithLIBNum("00000003a", "00000002a", 2), //StepNew 00000003a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000002a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000002a", "00000001a"), "00000002a"},
					},
				},
			},
		},
		{
			name:       "stalled",
			forkDB:     fdbLinked("00000001a"),
			startBlock: 3,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000002a", "00000001a", 1),
				bTestBlockWithLIBNum("00000003a", "00000002a", 2),
				bTestBlockWithLIBNum("00000003b", "00000002a", 2),
				bTestBlockWithLIBNum("00000004a", "00000003a", 3),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000002a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000002a", "00000001a", 1), "00000002a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000003a", "00000002a", 2), "00000003a"},
					},
				},
				{
					Step:      StepStalled,
					Obj:       "00000003b",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000003b", "00000002a", 2), "00000003b"},
					},
				},
			},
		},
		{
			name:       "undos error",
			forkDB:     fdbLinked("00000001a"),
			startBlock: 1,
			undoErr:    fmt.Errorf("error.1"),
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"), //StepNew 00000002a
				bTestBlock("00000003a", "00000002a"), //StepNew 00000003a
				bTestBlock("00000003b", "00000002a"), //nothing
				bTestBlock("00000004b", "00000003b"), //StepUndo 00000003a, StepNew 00000003b, StepNew 00000004b
			},
			expectedError:  "error.1",
			expectedResult: []*ForkableObject{},
		},
		{
			name:       "redos error",
			forkDB:     fdbLinked("00000001a"),
			startBlock: 1,
			redoErr:    fmt.Errorf("error.1"),
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"), //StepNew 00000002a
				bTestBlock("00000003a", "00000002a"), //StepNew 00000003a
				bTestBlock("00000003b", "00000002a"), //nothing
				bTestBlock("00000004b", "00000003b"), //StepUndo 00000003a, StepNew 00000003b, StepNew 00000004b
				bTestBlock("00000004a", "00000003a"), //nothing not longest chain
				bTestBlock("00000005a", "00000004a"), //StepUndo 00000004b, StepUndo 00000003b, StepRedos 00000003a, StepNew 00000004a
			},
			expectedError:  "error.1",
			expectedResult: []*ForkableObject{},
		},
		{
			name:   "out of order block",
			forkDB: fdbLinked("00000001a"),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000003b", "00000002a"), //nothing
			},
			expectedResult: []*ForkableObject{},
		},
		{
			name:   "start with a fork!",
			forkDB: fdbLinked("00000001a"),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002b", "00000001a"), //StepNew 00000002a
				bTestBlock("00000002a", "00000001a"), //Nothing
				bTestBlock("00000003a", "00000002a"), //StepNew 00000002a, StepNew 00000003a
				bTestBlock("00000004a", "00000003a"), //StepNew 00000004a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002b",
				},
				{
					Step:      StepUndo,
					Obj:       "00000002b",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000002b", "00000001a"), "00000002b"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
			},
		},
		{
			name:                               "ensure all blocks are new",
			forkDB:                             fdbLinked("00000001a"),
			ensureAllBlocksTriggerLongestChain: true,
			filterSteps:                        StepNew | StepIrreversible,
			startBlock:                         1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"),
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000003b", "00000002a"),
				bTestBlock("00000004b", "00000003b"),
				bTestBlock("00000004a", "00000003a"),
				bTestBlock("00000002b", "00000001a"),
				bTestBlockWithLIBNum("00000005b", "00000004b", 3),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000003b",
				},
				{
					Step: StepNew,
					Obj:  "00000004b",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000002b",
				},
				{
					Step: StepNew,
					Obj:  "00000005b",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000002a",
					StepCount: 2,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000002a", "00000001a"), "00000002a"},
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000003b",
					StepCount: 2,
					StepIndex: 1,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000002a", "00000001a"), "00000002a"},
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
			},
		},
		{
			name:                               "ensure all blocks are new with no holes",
			forkDB:                             fdbLinked("00000001a"),
			ensureAllBlocksTriggerLongestChain: true,
			filterSteps:                        StepNew,
			startBlock:                         1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"),
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000003b", "00000002a"),
				bTestBlock("00000004b", "00000003b"),
				bTestBlock("00000004a", "00000003a"),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000003b",
				},
				{
					Step: StepNew,
					Obj:  "00000004b",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
			},
		},
		{
			name:                               "ensure all blocks are new with holes skips some forked blocks",
			forkDB:                             fdbLinked("00000001a"),
			ensureAllBlocksTriggerLongestChain: true,
			filterSteps:                        StepNew,
			startBlock:                         1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"),
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000004b", "00000003b"),
				bTestBlock("00000003b", "00000002a"),
				bTestBlock("00000004a", "00000003a"),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				// {  // not there, because there was a hole in here.. :deng:
				// 	Step: StepNew,
				// 	Obj:  "00000004b",
				// },
				{
					Step: StepNew,
					Obj:  "00000003b",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
			},
		},
		{
			name:             "ensure block ID goes through preceded by hole",
			forkDB:           fdbLinked("00000001a"),
			ensureBlockFlows: bRef("00000004b"),
			filterSteps:      StepNew | StepUndo | StepRedo,
			startBlock:       1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"),
				bTestBlock("00000004b", "00000003a"),
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000004a", "00000003a"),
				bTestBlock("00000005a", "00000004a"),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000004b",
				},
				{
					Step:      StepUndo,
					Obj:       "00000004b",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000004b", "00000003a"), "00000004b"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000005a",
				},
			},
		},
		{
			name:             "ensure block ID goes through",
			forkDB:           fdbLinked("00000001a"),
			ensureBlockFlows: bRef("00000003b"),
			startBlock:       1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000002a", "00000001a"),
				bTestBlock("00000003a", "00000002a"),
				bTestBlock("00000004a", "00000003a"),
				bTestBlock("00000003b", "00000002a"),
				bTestBlock("00000005a", "00000004a"),
				bTestBlock("00000002b", "00000001a"),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003b",
				},
				{
					Step:      StepUndo,
					Obj:       "00000003b",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000005a",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := newTestForkableSink(c.undoErr, c.redoErr)

			fap := New(p)
			fap.forkDB = c.forkDB
			fap.ensureAllBlocksTriggerLongestChain = c.ensureAllBlocksTriggerLongestChain
			fap.includeInitialLIB = c.includeInitialLIB

			if c.ensureBlockFlows != nil {
				fap.ensureBlockFlows = c.ensureBlockFlows
			}

			if c.filterSteps != 0 {
				fap.filterSteps = c.filterSteps
			}

			var err error
			for _, b := range c.processBlocks {
				err = fap.ProcessBlock(b, b.ID())
			}
			if c.expectedError != "" {
				require.True(t, strings.HasSuffix(err.Error(), c.expectedError))
				return
			}

			for _, res := range c.expectedResult {
				res.ForkDB = c.forkDB
			}

			expected, err := json.MarshalIndent(c.expectedResult, "", "  ")
			require.NoError(t, err)
			result, err := json.MarshalIndent(p.results, "", "  ")
			require.NoError(t, err)

			// _ = expected
			// _ = result
			if !assert.Equal(t, string(expected), string(result)) {
				fmt.Println("Expected: ", string(expected))
				fmt.Println("result: ", string(result))
			}
		})
	}
}

func TestForkable_ProcessBlock_UnknownLIB(t *testing.T) {
	cases := []struct {
		name                string
		forkDB              *ForkDB
		processBlocks       []*bstream.Block
		undoErr             error
		redoErr             error
		startBlock          uint64
		expectedResultCount int
		expectedResult      []*ForkableObject
		expectedError       string
	}{
		{
			name:   "undos redos and skip",
			forkDB: fdbLinkedWithoutLIB(),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000002a", "00000001a", 1), //StepNew 00000002a
				bTestBlock("00000003a", "00000002a"),              //StepNew 00000003a
				bTestBlock("00000003b", "00000002a"),              //nothing
				bTestBlock("00000004b", "00000003b"),              //StepUndo 00000003a, StepNew 00000003b, StepNew 00000004b
				bTestBlock("00000004a", "00000003a"),              //nothing not longest chain
				bTestBlock("00000005a", "00000004a"),              //StepUndo 00000004b, StepUndo 00000003b, StepRedo 00000003a, StepNew 00000004a
				bTestBlock("00000007a", "00000006a"),              //nothing not longest chain
				bTestBlock("00000006a", "00000005a"),              //nothing
				bTestBlock("00000008a", "00000007a"),              //StepNew 00000007a, StepNew 00000008a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step:      StepUndo,
					Obj:       "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000003a", "00000002a"), "00000003a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000003b",
				},
				{
					Step: StepNew,
					Obj:  "00000004b",
				},
				{
					Step:      StepUndo,
					Obj:       "00000004b",
					StepCount: 2,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000004b", "00000003b"), "00000004b"},
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
				{
					Step:      StepUndo,
					Obj:       "00000003b",
					StepCount: 2,
					StepIndex: 1,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000004b", "00000003b"), "00000004b"},
						{bTestBlock("00000003b", "00000002a"), "00000003b"},
					},
				},
				{
					Step:      StepRedo,
					Obj:       "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlock("00000003a", "00000002a"), "00000003a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000005a",
				},
				{
					Step: StepNew,
					Obj:  "00000006a",
				},
				{
					Step: StepNew,
					Obj:  "00000007a",
				},
				{
					Step: StepNew,
					Obj:  "00000008a",
				},
			},
		},
		{
			name:       "irreversible",
			forkDB:     fdbLinkedWithoutLIB(),
			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000001a", "", 0),
				bTestBlockWithLIBNum("00000002a", "00000001a", 1), //StepNew 00000002a
				bTestBlockWithLIBNum("00000003a", "00000002a", 2), //StepNew 00000003a, StepIrreversible 2a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000002a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000002a", "00000001a", 1), "00000002a"},
					},
				},
			},
		},
		{
			name:       "stalled",
			forkDB:     fdbLinkedWithoutLIB(),
			startBlock: 3,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000002a", "00000001a", 1),
				bTestBlockWithLIBNum("00000003a", "00000002a", 2),
				bTestBlockWithLIBNum("00000003b", "00000002a", 2),
				bTestBlockWithLIBNum("00000004a", "00000003a", 3),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000002a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000002a", "00000001a", 1), "00000002a"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step:      StepIrreversible,
					Obj:       "00000003a",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000003a", "00000002a", 2), "00000003a"},
					},
				},
				{
					Step:      StepStalled,
					Obj:       "00000003b",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000003b", "00000002a", 2), "00000003b"},
					},
				},
			},
		},
		{
			name:       "undos error",
			forkDB:     fdbLinkedWithoutLIB(),
			startBlock: 1,
			undoErr:    fmt.Errorf("error.1"),
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000002a", "00000001a", 1),
				bTestBlockWithLIBNum("00000003a", "00000002a", 1),
				bTestBlockWithLIBNum("00000003b", "00000002a", 1),
				bTestBlockWithLIBNum("00000004b", "00000003b", 1),
			},
			expectedError:  "error.1",
			expectedResult: []*ForkableObject{},
		},
		{
			name:       "redos error",
			forkDB:     fdbLinkedWithoutLIB(),
			startBlock: 1,
			redoErr:    fmt.Errorf("error.1"),
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000002a", "00000001a", 1), //StepNew 00000002a
				bTestBlockWithLIBNum("00000003a", "00000002a", 1), //StepNew 00000003a
				bTestBlockWithLIBNum("00000003b", "00000002a", 1), //nothing
				bTestBlockWithLIBNum("00000004b", "00000003b", 1), //StepUndo 00000003a, StepNew 00000003b, StepNew 00000004b
				bTestBlockWithLIBNum("00000004a", "00000003a", 1), //nothing not longest chain
				bTestBlockWithLIBNum("00000005a", "00000004a", 1), //StepUndo 00000004b, StepUndo 00000003b, StepRedos 00000003a, StepNew 00000004a
			},
			expectedError:  "error.1",
			expectedResult: []*ForkableObject{},
		},
		{
			name:       "out of order block",
			forkDB:     fdbLinkedWithoutLIB(),
			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlock("00000003b", "00000002a"), //nothing
			},
			expectedResult: []*ForkableObject{},
		},
		{
			name:       "start with a fork!",
			forkDB:     fdbLinkedWithoutLIB(),
			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000002b", "00000001a", 1), //StepNew 00000002a
				bTestBlockWithLIBNum("00000002a", "00000001a", 1), //Nothing
				bTestBlockWithLIBNum("00000003a", "00000002a", 1), //StepNew 00000002a, StepNew 00000003a, StepIrreversible 2a
				bTestBlockWithLIBNum("00000004a", "00000003a", 1), //StepNew 00000004a
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000002b",
				},
				{
					Step:      StepUndo,
					Obj:       "00000002b",
					StepCount: 1,
					StepIndex: 0,
					StepBlocks: []*bstream.PreprocessedBlock{
						{bTestBlockWithLIBNum("00000002b", "00000001a", 1), "00000002b"},
					},
				},
				{
					Step: StepNew,
					Obj:  "00000002a",
				},
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
			},
		},
		{
			name:   "validate cannot go up to dposnum to set lib",
			forkDB: fdbLinkedWithoutLIB(),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000004a", "00000003a", 1),
				bTestBlockWithLIBNum("00000005a", "00000004a", 2),
			},
			expectedResult: []*ForkableObject{},
		},
		{
			name:   "validate we can set LIB to ID referenced as Previous and start sending after",
			forkDB: fdbLinkedWithoutLIB(),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000003b", "00000002a", 1),
				bTestBlockWithLIBNum("00000003a", "00000002a", 1),
				bTestBlockWithLIBNum("00000004a", "00000003a", 2),
				bTestBlockWithLIBNum("00000005a", "00000004a", 2),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000003a",
				},
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000005a",
				},
			},
		},
		{
			name:   "validate we can set LIB to ID actually seen and start sending after, with burst",
			forkDB: fdbLinkedWithoutLIB(),

			startBlock: 1,
			processBlocks: []*bstream.Block{
				bTestBlockWithLIBNum("00000003a", "00000002a", 1),
				bTestBlockWithLIBNum("00000004a", "00000003a", 1),
				bTestBlockWithLIBNum("00000004b", "00000003a", 1),
				bTestBlockWithLIBNum("00000005a", "00000004a", 3),
			},
			expectedResult: []*ForkableObject{
				{
					Step: StepNew,
					Obj:  "00000004a",
				},
				{
					Step: StepNew,
					Obj:  "00000005a",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := newTestForkableSink(c.undoErr, c.redoErr)

			fap := New(p)
			fap.forkDB = c.forkDB

			var err error
			for _, b := range c.processBlocks {
				err = fap.ProcessBlock(b, b.ID())
				if err != nil {
					break
				}
			}
			if c.expectedError != "" {
				require.Error(t, err)
				require.True(t, strings.HasSuffix(err.Error(), c.expectedError))
				return
			} else {
				require.NoError(t, err)
			}

			for _, res := range c.expectedResult {
				res.ForkDB = c.forkDB
			}

			expected, err := json.Marshal(c.expectedResult)
			require.NoError(t, err)
			result, err := json.Marshal(p.results)
			require.NoError(t, err)

			_ = expected
			_ = result
			if !assert.Equal(t, c.expectedResult, p.results) {
				fmt.Println("Expected: ", string(expected))
				fmt.Println("result: ", string(result))
			}
		})
	}
}

func TestForkable_ForkDBContainsPreviousPreprocessedBlockObjects(t *testing.T) {
	var nilHandler bstream.Handler
	p := New(nilHandler, WithExclusiveLIB(bRef("00000003a")))

	err := p.ProcessBlock(bTestBlock("00000004a", ""), "mama")
	require.NoError(t, err)

	blk := p.forkDB.BlockForID("00000004a")
	assert.Equal(t, "mama", blk.Object.(*ForkableBlock).Obj)
}

func TestComputeNewLongestChain(t *testing.T) {
	p := &Forkable{
		forkDB:           NewForkDB(),
		ensureBlockFlows: bstream.BlockRefEmpty,
	}

	p.forkDB.MoveLIB(bRef("00000001a"))

	p.forkDB.AddLink(bRef("00000002a"), bRef("00000001a"), simplePpBlock("00000002a", "00000001a"))
	longestChain := p.computeNewLongestChain(simplePpBlock("00000002a", "00000001a"))
	expected := []*Block{
		simpleFdbBlock("00000002a", "00000001a"),
	}
	assert.Equal(t, expected, longestChain, "initial computing of longest chain")

	p.forkDB.AddLink(bRef("00000003a"), bRef("00000002a"), simplePpBlock("00000003a", "00000002a"))
	longestChain = p.computeNewLongestChain(simplePpBlock("00000003a", "00000002a"))
	expected = []*Block{
		simpleFdbBlock("00000002a", "00000001a"),
		simpleFdbBlock("00000003a", "00000002a"),
	}
	assert.Equal(t, expected, longestChain, "adding a block to longest chain computation")

	p.forkDB.MoveLIB(bRef("00000003a"))
	p.forkDB.AddLink(bRef("00000004a"), bRef("00000003a"), simplePpBlock("00000004a", "00000003a"))
	longestChain = p.computeNewLongestChain(simplePpBlock("00000004a", "00000003a"))
	expected = []*Block{
		simpleFdbBlock("00000004a", "00000003a"),
	}
	assert.Equal(t, expected, longestChain, "recalculating longest chain if lib changed")
}

func simplePpBlock(id, previous string) *ForkableBlock {
	return &ForkableBlock{
		Block: bTestBlock(id, previous),
	}
}

func simpleFdbBlock(id, previous string) *Block {
	return &Block{
		BlockID:  id,
		BlockNum: uint64(eos.BlockNum(id)),
		Object:   simplePpBlock(id, previous),
	}
}

func TestForkableSentChainSwitchSegments(t *testing.T) {
	p := &Forkable{
		forkDB:           NewForkDB(),
		ensureBlockFlows: bstream.BlockRefEmpty,
	}
	p.forkDB.AddLink(bRef("00000003a"), bRef("00000002a"), nil)
	p.forkDB.AddLink(bRef("00000002a"), bRef("00000001a"), nil)

	undos, redos := p.sentChainSwitchSegments(zlog, "00000003a", "00000003a")
	assert.Nil(t, undos)
	assert.Nil(t, redos)
}
