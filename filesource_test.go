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

	"github.com/streamingfast/dstore"
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

func base(in int) string {
	return fmt.Sprintf("%010d", in)
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

	fs := NewFileSource(TestChainConfig().FirstStreamableBlock, bs, 1, 1, preprocessor, handler)
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
