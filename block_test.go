package bstream

import (
	"github.com/davecgh/go-spew/spew"
	"github.com/golang/protobuf/proto"
	atm "github.com/streamingfast/atm"
	"github.com/streamingfast/bstream/caching"
	"github.com/streamingfast/bstream/decoding"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"math"
	"testing"
)

func TestBlock_ToAny(t *testing.T) {
	diskCache := atm.NewCache("/tmp", math.MaxInt, math.MaxInt, atm.NewFileIO())
	engine := caching.NewCacheEngine("test", diskCache)
	caching.Engine = engine

	GetBlockDecoder = decoding.BlockDecoderFunc(func(data []byte) (proto.Message, error) {
		blockRef := &pbbstream.BlockRef{}
		err := proto.Unmarshal(data, blockRef)
		require.NoError(t, err)
		return blockRef, nil
	})

	blockRef := &pbbstream.BlockRef{
		Num: 101,
		Id:  "101a",
	}

	payload, err := proto.Marshal(blockRef)
	require.NoError(t, err)

	pbBlock := &pbbstream.Block{
		Number:         101,
		Id:             "101a",
		PreviousId:     "100a",
		Timestamp:      timestamppb.Now(),
		LibNum:         99,
		PayloadKind:    0,
		PayloadVersion: 0,
		PayloadBuffer:  payload,
	}
	block, err := NewBlockFromProto(pbBlock)
	require.NoError(t, err)
	any, err := block.ToAny(true, nil)
	require.NoError(t, err)
	spew.Dump(any)
}
