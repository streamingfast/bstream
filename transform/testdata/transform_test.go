package pbtransform

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/decoding"
	"github.com/streamingfast/bstream/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"testing"
)

func blockIncrementorTransform(t *testing.T) *anypb.Any {
	transform := &BlockNumberSquare{}
	a, err := anypb.New(transform)
	require.NoError(t, err)
	return a
}

func init() {
	// registering transforms
	transform.Register(&BlockNumberSquare{}, TestBlockSquareTransformFactory)
	bstream.GetBlockDecoder = decoding.BlockDecoderFunc(func(blk *bstream.Block) (interface{}, error) {
		return blk, nil
	})

}
func TestBuildFromTransforms(t *testing.T) {
	transforms := []*anypb.Any{blockIncrementorTransform(t)}
	blk := bstream.TestBlock("00000002a", "00000001a")

	preprocFunc, err := transform.BuildFromTransforms(transforms)
	require.NoError(t, err)
	output, err := preprocFunc(blk)
	require.NoError(t, err)
	out := output.(*BlockNumberSquareOutput)
	assert.Equal(t, uint64(4), out.Square)
}
