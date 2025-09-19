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
	"bytes"
	"strings"
	"testing"

	pbtest "github.com/streamingfast/bstream/internal/pb/sf/bstream/test/v1"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestOneBlocksSource(t *testing.T) {
	store := dstore.NewMockStore(nil)
	AddToMockStore(t, store,
		&pbtest.Block{Id: "1a"},
		&pbtest.Block{Id: "2a"},
		&pbtest.Block{Id: "3a"},
		&pbtest.Block{Id: "3a_b"},
		&pbtest.Block{Id: "4b"},
		&pbtest.Block{Id: "5b"},
	)

	recorder := &oneBlockRecorder{T: t}
	source, err := NewOneBlocksSource(0, store, recorder, OneBlocksSourceLogger(zlogTest))
	require.NoError(t, err)

	err = source.run()
	require.NoError(t, err)

	require.Len(t, recorder.blocks, 6)
	require.Equal(t, "1a", recorder.blocks[0].Id)
	require.Equal(t, "2a", recorder.blocks[1].Id)
	require.Equal(t, "3a", recorder.blocks[2].Id)
	require.Equal(t, "3a_b", recorder.blocks[3].Id)
	require.Equal(t, "4b", recorder.blocks[4].Id)
	require.Equal(t, "5b", recorder.blocks[5].Id)
}

func TestOneBlocksSource_LowestNumSet(t *testing.T) {
	store := dstore.NewMockStore(nil)
	AddToMockStore(t, store,
		&pbtest.Block{Id: "1a"},
		&pbtest.Block{Id: "2a"},
		&pbtest.Block{Id: "3a"},
		&pbtest.Block{Id: "3a_b"},
		&pbtest.Block{Id: "4b"},
		&pbtest.Block{Id: "5b"},
	)

	recorder := &oneBlockRecorder{T: t}
	source, err := NewOneBlocksSource(4, store, recorder, OneBlocksSourceLogger(zlogTest))
	require.NoError(t, err)

	err = source.run()
	require.NoError(t, err)

	require.Len(t, recorder.blocks, 2)
	require.Equal(t, "4b", recorder.blocks[0].Id)
	require.Equal(t, "5b", recorder.blocks[1].Id)
}

func TestOneBlocksSource_SkipperFuncSet(t *testing.T) {
	store := dstore.NewMockStore(nil)
	AddToMockStore(t, store,
		&pbtest.Block{Id: "1a"},
		&pbtest.Block{Id: "2a"},
		&pbtest.Block{Id: "3a_b"},
		&pbtest.Block{Id: "3a"},
		&pbtest.Block{Id: "4b"},
		&pbtest.Block{Id: "5b"},
	)

	recorder := &oneBlockRecorder{T: t}
	source, err := NewOneBlocksSource(1, store, recorder, OneBlocksSourceLogger(zlogTest), OneBlocksSourceWithSkipperFunc(func(blockID string) bool {
		return strings.HasSuffix(blockID, "b")
	}))
	require.NoError(t, err)

	err = source.run()
	require.NoError(t, err)

	require.Len(t, recorder.blocks, 3)
	require.Equal(t, "1a", recorder.blocks[0].Id)
	require.Equal(t, "2a", recorder.blocks[1].Id)
	require.Equal(t, "3a", recorder.blocks[2].Id)
}

func AddToMockStore(t *testing.T, store *dstore.MockStore, blocks ...*pbtest.Block) {
	t.Helper()

	for _, block := range blocks {
		buffer := bytes.NewBuffer(nil)
		blockWriter, err := NewDBinBlockWriter(buffer)
		require.NoError(t, err)

		pbbstreamBlock := block.ToPbbstreamBlock()

		err = blockWriter.Write(pbbstreamBlock)
		require.NoError(t, err)

		store.SetFile(BlockFileName(pbbstreamBlock), buffer.Bytes())
	}
}

type oneBlockRecorder struct {
	*testing.T
	blocks []*pbtest.Block
}

func (r *oneBlockRecorder) ProcessSignal(_ *pbbstream.Signal) error {
	return nil
}

func (r *oneBlockRecorder) ProcessBlock(blk *pbbstream.Block, obj any) error {
	block := &pbtest.Block{}
	err := anypb.UnmarshalTo(blk.Payload, block, proto.UnmarshalOptions{})
	require.NoError(r.T, err)

	r.blocks = append(r.blocks, block)
	return nil
}
