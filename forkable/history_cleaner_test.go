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

package forkable

import (
	"fmt"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/assert"
)

func TestForkable_HistoryCleaner(t *testing.T) {
	cases := []struct {
		name            string
		historyCliff    func(ref bstream.BlockRef) bool
		processBlocks   []*bstream.Block
		expected        []string // {blockID}-{stepName}
		expectedCursors []string
	}{
		{
			"everything by stepd",
			func(ref bstream.BlockRef) bool {
				return ref.Num() >= 5
			},
			[]*bstream.Block{
				bTestBlockWithLIBNum("00000001a", "00000000a", 0),
				bTestBlockWithLIBNum("00000002a", "00000001a", 1), //2 NEW
				bTestBlockWithLIBNum("00000003a", "00000002a", 1), //3 NEW
				bTestBlockWithLIBNum("00000004a", "00000003a", 2), //4 NEW, 2 IRR
				bTestBlockWithLIBNum("00000004b", "00000003a", 2), //nada
				bTestBlockWithLIBNum("00000004c", "00000003a", 3), //nada
				bTestBlockWithLIBNum("00000005b", "00000004b", 3), //4a UNDO, 4b NEW, 5 NEW, 3 IRR
				bTestBlockWithLIBNum("00000006b", "00000005b", 4), //6 NEW, 4 IRR
				bTestBlockWithLIBNum("00000007b", "00000006b", 5), //7 NEW, 5 IRR (triggers histcleaner passthrough)
				bTestBlockWithLIBNum("00000008b", "00000007b", 6), //8 NEW, 6 IRR
				bTestBlockWithLIBNum("00000007c", "00000006b", 6), //nada
				bTestBlockWithLIBNum("00000008c", "00000007c", 6), //nada
				bTestBlockWithLIBNum("00000009c", "00000008c", 6), //8b UNDO, 7b UNDO, 7c NEW, 8c NEW, 9c NEW
			},
			[]string{
				"00000001a-new",
				"00000001a-irreversible",
				"00000002a-new",
				"00000002a-irreversible",
				"00000003a-new",
				"00000003a-irreversible",
				"00000004b-new",
				"00000004b-irreversible",
				"00000005b-new",
				"00000005b-irreversible",
				"00000006b-new",
				"00000007b-new",
				"00000008b-new",
				"00000006b-irreversible",
				"00000008b-undo",
				"00000007b-undo",
				"00000007c-new",
				"00000008c-new",
				"00000009c-new",
			},
			[]string{
				"c1:1:1:00000001a:1:00000001a",
				"c1:16:1:00000001a:1:00000001a",
				"c2:1:2:00000002a:4:00000004a",
				"c2:16:2:00000002a:4:00000004a",
				"c2:1:3:00000003a:5:00000005b",
				"c2:16:3:00000003a:5:00000005b",
				"c2:1:4:00000004b:6:00000006b",
				"c2:16:4:00000004b:6:00000006b",
				"c2:1:5:00000005b:7:00000007b",
				"c2:16:5:00000005b:7:00000007b",
				"c1:1:6:00000006b:5:00000005b", // different from source but valid
				"c1:1:7:00000007b:5:00000005b", // different from source but valid
				"c1:1:8:00000008b:5:00000005b",
				"c2:16:6:00000006b:8:00000008b", // different from source but valid ???
				"c3:2:8:00000008b:9:00000009c:6:00000006b",
				"c3:2:7:00000007b:9:00000009c:6:00000006b",
				"c3:1:7:00000007c:9:00000009c:6:00000006b",
				"c3:1:8:00000008c:9:00000009c:6:00000006b",
				"c1:1:9:00000009c:6:00000006b",
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var received []string
			var receivedCursors []string

			handler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
				received = append(received, fmt.Sprintf("%s-%s", blk.ID(), obj.(*ForkableObject).Step.String()))
				receivedCursors = append(receivedCursors, obj.(*ForkableObject).Cursor().String())
				return nil
			})



			hist := NewHistoryCleaner(c.historyCliff, handler)
			fkable := New(hist, WithInclusiveLIB(bstream.NewBlockRef("00000001a", 1)))

			for _, b := range c.processBlocks {
				fkable.ProcessBlock(b, nil)
			}

			assert.Equal(t, c.expected, received)
			assert.Equal(t, c.expectedCursors, receivedCursors)
		})
	}

}
