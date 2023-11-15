package pbtransform

import (
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/transform"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
)

func blockIncrementorTransform(t *testing.T) *anypb.Any {
	transform := &BlockNumberSquare{}
	a, err := anypb.New(transform)
	require.NoError(t, err)
	return a
}

var registry *transform.Registry

func init() {
	// registering transforms
	registry = transform.NewRegistry()
	registry.Register(&transform.Factory{
		&BlockNumberSquare{},
		TestBlockSquareTransformFactory},
	)

}
func TestBuildFromTransforms(t *testing.T) {
	transforms := []*anypb.Any{blockIncrementorTransform(t)}
	blk := bstream.TestBlock("00000002a", "00000001a")

	preprocFunc, indexProvider, desc, err := registry.BuildFromTransforms(transforms)
	require.NoError(t, err)
	output, err := preprocFunc(blk)
	require.NoError(t, err)
	out := output.(*BlockNumberSquareOutput)
	assert.Equal(t, uint64(4), out.Square)
	assert.Nil(t, indexProvider)
	assert.Equal(t, "block_square_transform", desc)
}
