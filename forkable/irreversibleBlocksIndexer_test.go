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
	"encoding/json"
	"io"
	"io/ioutil"
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func Test_IrreversibleBlocksIndexer(t *testing.T) {

	cases := []struct {
		name          string
		forkDB        *ForkDB
		bundleSizes   []uint64
		processBlocks []*bstream.Block
		expectIndexes map[string][]*pbblockmeta.BlockRef
	}{
		{
			"single bundle size",
			fdbLinked("3a"),
			[]uint64{2},
			[]*bstream.Block{
				tb("3a", "2a", 0),
				tb("4a", "3a", 0),
				tb("5a", "4a", 3),
				tb("6a", "5a", 4),
				tb("7a", "6a", 5),
				tb("8a", "7a", 6),
				tb("9a", "8a", 7),
				tb("aa", "9a", 8),
			},
			map[string][]*pbblockmeta.BlockRef{
				"0000000004.2.irr.idx": {{BlockNum: 4, BlockID: "4a"}, {BlockNum: 5, BlockID: "5a"}},
				"0000000006.2.irr.idx": {{BlockNum: 6, BlockID: "6a"}, {BlockNum: 7, BlockID: "7a"}},
			},
		},
		{
			"two bundle sizes",
			fdbLinked("2a"),
			[]uint64{2, 3},
			[]*bstream.Block{
				tb("3a", "2a", 0),
				tb("4a", "3a", 0),
				tb("5a", "4a", 3),
				tb("6a", "5a", 4),
				tb("7a", "6a", 5),
				tb("8a", "7a", 6),
				tb("9a", "8a", 7),
				tb("aa", "9a", 8),
				tb("ba", "aa", 9),
				tb("ca", "ba", 10),
			},
			map[string][]*pbblockmeta.BlockRef{
				"0000000004.2.irr.idx": {{BlockNum: 4, BlockID: "4a"}, {BlockNum: 5, BlockID: "5a"}},
				"0000000006.2.irr.idx": {{BlockNum: 6, BlockID: "6a"}, {BlockNum: 7, BlockID: "7a"}},
				"0000000008.2.irr.idx": {{BlockNum: 8, BlockID: "8a"}, {BlockNum: 9, BlockID: "9a"}},
				"0000000003.3.irr.idx": {{BlockNum: 3, BlockID: "3a"}, {BlockNum: 4, BlockID: "4a"}, {BlockNum: 5, BlockID: "5a"}},
				"0000000006.3.irr.idx": {{BlockNum: 6, BlockID: "6a"}, {BlockNum: 7, BlockID: "7a"}, {BlockNum: 8, BlockID: "8a"}},
			},
		},
		{
			"incomplete beginning",
			fdbLinked("3a"),
			[]uint64{3},
			[]*bstream.Block{
				tb("3a", "2a", 0),
				tb("4a", "3a", 0),
				tb("5a", "4a", 3),
				tb("6a", "5a", 4),
				tb("7a", "6a", 5),
				tb("8a", "7a", 6),
			},
			map[string][]*pbblockmeta.BlockRef{},
		},
		{
			"hole after beginning",
			fdbLinked("3a"),
			[]uint64{2},
			[]*bstream.Block{
				tb("4a", "3a", 0),
				tb("6a", "4a", 3), // skip 5
				tb("7a", "6a", 4),
				tb("8a", "7a", 6),
			},
			map[string][]*pbblockmeta.BlockRef{
				"0000000004.2.irr.idx": {{BlockNum: 4, BlockID: "4a"}},
			},
		},
		{
			"big hole",
			fdbLinked("3a"),
			[]uint64{2},
			[]*bstream.Block{
				tb("4a", "3a", 3),
				tb("8a", "4a", 4), // skip 5, 6, 7
				tb("9a", "8a", 8),
				tb("aa", "9a", 9),
				tb("ba", "aa", 10),
			},
			map[string][]*pbblockmeta.BlockRef{
				"0000000004.2.irr.idx": {{BlockNum: 4, BlockID: "4a"}},
				"0000000008.2.irr.idx": {{BlockNum: 8, BlockID: "8a"}, {BlockNum: 9, BlockID: "9a"}},
			},
		},

		{
			"holes after first complete",
			fdbLinked("3a"),
			[]uint64{2},
			[]*bstream.Block{
				tb("4a", "3a", 0),
				tb("5a", "4a", 3),
				tb("7a", "5a", 4),
				tb("8a", "7a", 6),
				tb("9a", "8a", 7),
				tb("aa", "9a", 8),
				tb("8a", "7a", 5),
			},
			map[string][]*pbblockmeta.BlockRef{
				"0000000004.2.irr.idx": {{BlockNum: 4, BlockID: "4a"}, {BlockNum: 5, BlockID: "5a"}},
				"0000000006.2.irr.idx": {{BlockNum: 7, BlockID: "7a"}},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := newTestForkableSink(nil, nil)
			bstream.GetProtocolFirstStreamableBlock = 0

			indexes := make(map[string][]*pbblockmeta.BlockRef)

			store := dstore.NewMockStore(func(base string, f io.Reader) (err error) {
				bts, err := ioutil.ReadAll(f)
				require.NoError(t, err)

				resp := &pbblockmeta.BlockRefs{}
				err = proto.Unmarshal(bts, resp)
				require.NoError(t, err)
				indexes[base] = resp.BlockRefs
				return nil
			})

			irrBlkIndexer := NewIrreversibleBlocksIndexer(store, c.bundleSizes, p)
			fap := New(irrBlkIndexer)
			fap.forkDB = c.forkDB
			if fap.forkDB.HasLIB() {
				fap.lastLIBSeen = bstream.NewBlockRef(fap.forkDB.libID, fap.forkDB.libNum)
			}

			var err error
			for _, b := range c.processBlocks {
				err = fap.ProcessBlock(b, b.ID())
				require.NoError(t, err)
			}

			actual, err := json.Marshal(indexes)
			require.NoError(t, err)
			assert.EqualValues(t, c.expectIndexes, indexes, string(actual))
		})
	}
}
