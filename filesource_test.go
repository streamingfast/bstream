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
	"testing"
	"time"

	"github.com/dfuse-io/dstore"
	"github.com/stretchr/testify/require"
)

func TestFileSource_Run(t *testing.T) {
	bs := dstore.NewMockStore(nil)
	bs.SetFile(`0000000000`, []byte(`{"id":"00000001a"}
{"id":"00000002a"}
`))
	expectedBlockCount := 2
	preProcessCount := 0
	preprocessor := PreprocessFunc(func(blk *Block) (interface{}, error) {
		preProcessCount++
		require.Equal(t, uint64(preProcessCount), blk.Num())
		return blk.ID(), nil
	})

	testDone := make(chan interface{})
	handlerCount := 0
	handler := HandlerFunc(func(blk *Block, obj interface{}) error {
		handlerCount++
		require.Equal(t, uint64(handlerCount), blk.Num())
		require.Equal(t, blk.ID(), obj)
		if handlerCount >= expectedBlockCount {
			close(testDone)
		}
		return nil
	})

	fs := NewFileSource( bs, 1, 1, preprocessor, handler)
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
