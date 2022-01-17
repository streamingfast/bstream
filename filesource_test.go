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

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/dstore"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryableError(t *testing.T) {
	err := fmt.Errorf("hehe")
	ret := retryableError{err}
	require.True(t, isRetryable(ret))
	require.False(t, isRetryable(err))
	require.Equal(t, ret.Error(), err.Error())
}

func testBlocks(in ...interface{}) (out []byte) {
	var blks []ParsableTestBlock
	for i := 0; i < len(in); i += 4 {
		blks = append(blks, ParsableTestBlock{
			Number:     uint64(in[i].(int)),
			ID:         in[i+1].(string),
			PreviousID: in[i+2].(string),
			LIBNum:     uint64(in[i+3].(int)),
		})
	}

	for _, blk := range blks {
		b, err := json.Marshal(blk)
		if err != nil {
			panic(err)
		}
		out = append(out, b...)
		out = append(out, '\n')
	}
	return
}

func testIrrBlocksIdx(baseNum, bundleSize int, numToID map[int]string) (filename string, content []byte) {
	filename = fmt.Sprintf("%010d.%d.irr.idx", baseNum, bundleSize)

	var blockrefs []*pbblockmeta.BlockRef

	for i := baseNum; i < baseNum+bundleSize; i++ {
		if id, ok := numToID[i]; ok {
			blockrefs = append(blockrefs, &pbblockmeta.BlockRef{
				BlockNum: uint64(i),
				BlockID:  id,
			})
		}

	}

	var err error
	content, err = proto.Marshal(&pbblockmeta.BlockRefs{
		BlockRefs: blockrefs,
	})
	if err != nil {
		panic(err)
	}

	return
}

func base(in int) string {
	return fmt.Sprintf("%010d", in)
}

func TestFileSource_IrrIndex(t *testing.T) {

	tests := []struct {
		name                      string
		files                     map[int][]byte
		irreversibleBlocksIndexes map[int]map[int]map[int]string
		expectedBlockIDs          []string
		bundleSizes               []uint64
		cursorBlockRef            *BasicBlockRef
	}{
		{
			"skip forked blocks",
			map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
				6, "6b", "4a", 0,
				99, "99a", "6a", 0,
			),
			},
			map[int]map[int]map[int]string{
				100: {
					0: {
						4: "4a",
						6: "6a",
					},
				},
			},
			[]string{
				"4a",
				"6a",
			},
			[]uint64{100},
			&BasicBlockRef{"", 1},
		},
		{
			"send everything if no index",
			map[int][]byte{0: testBlocks(
				4, "4a", "3a", 0,
				6, "6a", "4a", 0,
				6, "6b", "4a", 0,
				99, "99a", "6a", 0,
			),
			},
			nil,
			[]string{
				"4a",
				"6a",
				"6b",
				"99a",
			},
			[]uint64{100},
			&BasicBlockRef{"", 1},
		},
		{
			"skip everything if empty index",
			map[int][]byte{
				0: testBlocks(
					4, "4a", "3a", 0,
					6, "6a", "4a", 0,
				),
				100: testBlocks(
					100, "100a", "zz", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {},
					100: {100: "100a"},
				},
			},
			[]string{"100a"},
			[]uint64{100},
			&BasicBlockRef{"", 1},
		},
		{
			"transition to unindexed range",
			map[int][]byte{
				0: testBlocks(
					4, "4a", "zz", 0,
					6, "6a", "4a", 0,
				),
				100: testBlocks(
					100, "100a", "6a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0: {4: "4a", 6: "6a"},
				},
			},
			[]string{"4a", "6a", "100a"},
			[]uint64{100},
			&BasicBlockRef{"", 1},
		},
		{
			"never transition back to indexed range",
			map[int][]byte{
				0: testBlocks(
					4, "4a", "zz", 0,
					6, "6a", "4a", 0,
				),
				100: testBlocks(
					100, "100a", "6a", 0,
					100, "100b", "6a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					100: {100: "100a"},
				},
			},
			[]string{"4a", "6a", "100a", "100b"},
			[]uint64{100},
			&BasicBlockRef{"", 1},
		},
		{
			"irreversible blocks in next block file",
			map[int][]byte{
				0: testBlocks(
					1, "1a", "0a", 0,
					50, "50a", "1a", 0,
					75, "75b", "50a", 0,
				),
				100: testBlocks(
					75, "75a", "50a", 0, // this DOES happen with forky chains
					100, "100a", "75a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {1: "1a", 50: "50a", 75: "75a"},
					100: {100: "100a"},
				},
			},
			[]string{"1a", "50a", "75a", "100a"},
			[]uint64{100},
			&BasicBlockRef{"", 1},
		},
		{
			"large file takes precedence over small files",
			map[int][]byte{
				0: testBlocks(
					1, "1a", "0a", 0,
					50, "50a", "1a", 0,
				),
				100: testBlocks(
					100, "100a", "50a", 0,
				),
				200: testBlocks(
					200, "200a", "50a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {1: "1a", 50: "50a"},
					100: {100: "NONEXISTING"}, // this is not a real life scenario, only for testing
				},
				1000: {
					0: {1: "1a", 50: "50a", 200: "200a"},
				},
			},
			[]string{"1a", "50a", "200a"},
			[]uint64{100, 1000},
			&BasicBlockRef{"", 1},
		},
		{
			"don't use index if cursor is on a forked block",
			map[int][]byte{
				0: testBlocks(
					2, "2a", "1a", 0,
					2, "2b", "1a", 0,
					3, "3a", "2a", 0,
				),
				100: testBlocks(
					100, "100a", "3a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {2: "2a", 3: "3a"},
					100: {100: "100a"},
				},
			},
			[]string{"2a", "2b", "3a", "100a"},
			[]uint64{100},
			&BasicBlockRef{"2b", 2},
		},
		{
			"use index if cursor is on a valid block",
			map[int][]byte{
				0: testBlocks(
					2, "2a", "1a", 0,
					2, "2b", "1a", 0,
					3, "3a", "2a", 0,
				),
				100: testBlocks(
					100, "100a", "3a", 0,
				),
			},
			map[int]map[int]map[int]string{
				100: {
					0:   {2: "2a", 3: "3a"},
					100: {100: "100a"},
				},
			},
			[]string{"2a", "3a", "100a"},
			[]uint64{100},
			&BasicBlockRef{"2a", 2},
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			bs := dstore.NewMockStore(nil)
			for i, data := range c.files {
				bs.SetFile(base(i), data)
			}

			irrStore := dstore.NewMockStore(nil)

			for i, m := range c.irreversibleBlocksIndexes {
				for j, n := range m {
					filename, cnt := testIrrBlocksIdx(j, i, n)
					irrStore.SetFile(filename, cnt)
				}

			}
			expectedBlockCount := len(c.expectedBlockIDs)

			var receivedBlockIDs []string

			testDone := make(chan interface{})
			handler := HandlerFunc(func(blk *Block, obj interface{}) error {
				receivedBlockIDs = append(receivedBlockIDs, blk.Id)
				if len(receivedBlockIDs) == expectedBlockCount {
					close(testDone)
				}
				return nil
			})

			startBlockNum := uint64(1)
			if c.cursorBlockRef != nil {
				startBlockNum = c.cursorBlockRef.num
			}
			var mustMatch BlockRef
			if c.cursorBlockRef != nil {
				mustMatch = c.cursorBlockRef
			}
			fs := NewFileSource(bs, startBlockNum, 1, nil, handler, FileSourceWithSkipForkedBlocks(irrStore, c.bundleSizes, mustMatch))
			go fs.Run()

			select {
			case <-testDone:
			case <-time.After(100 * time.Millisecond):
				t.Error(fmt.Sprintf("Test timeout waiting for %d blocks", expectedBlockCount))
			}

			assert.EqualValues(t, c.expectedBlockIDs, receivedBlockIDs)
			fs.Shutdown(nil)

		})
	}

}

func TestFileSource_Run(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(base(0), testBlocks(
		1, "1a", "", 0,
		2, "2a", "", 0,
	))
	bs.SetFile(base(100), testBlocks(
		3, "3a", "", 0,
		4, "4a", "", 0,
	))

	expectedBlockCount := 4
	preProcessCount := 0
	preprocessor := PreprocessFunc(func(blk *Block) (interface{}, error) {
		preProcessCount++
		return blk.ID(), nil
	})

	testDone := make(chan interface{})
	handlerCount := 0
	expectedBlockNum := uint64(1)
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		zlog.Debug("test : received block", zap.Stringer("block_ref", blk))
		require.Equal(t, expectedBlockNum, blk.Number)
		expectedBlockNum++
		handlerCount++
		require.Equal(t, uint64(handlerCount), blk.Num())
		require.Equal(t, blk.ID(), obj)
		if handlerCount >= expectedBlockCount {
			close(testDone)
		}
		return nil
	})

	fs := NewFileSource(bs, 1, 1, preprocessor, handler)
	go fs.Run()

	select {
	case <-testDone:
		require.Equal(t, expectedBlockCount, preProcessCount)
		require.Equal(t, expectedBlockCount, handlerCount)
	case <-time.After(100 * time.Millisecond):
		t.Error("Test timeout")
	}
	fs.Shutdown(nil)
}
