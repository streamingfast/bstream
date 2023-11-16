package pbtransform

import (
	"fmt"

	"github.com/streamingfast/bstream/transform"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const TestFilterTransformTypeURL = "sf.bstream.transforms.v1.BlockNumberSquare"

var TestBlockSquareTransformFactory = func(message *anypb.Any) (transform.Transform, error) {
	mname := message.MessageName()
	if mname != TestFilterTransformTypeURL {
		return nil, fmt.Errorf("expected type url %q, recevied %q ", TestFilterTransformTypeURL, message.TypeUrl)
	}

	var t BlockNumberSquare
	err := proto.Unmarshal(message.Value, &t)
	if err != nil {
		return nil, fmt.Errorf("unexpected unmarshall error %w", err)
	}

	return &BlockSquareTransform{}, nil
}

type BlockSquareTransform struct{}

func (t *BlockSquareTransform) validateInput(in transform.Input) bool {
	return in.Type() == transform.NilObjectType
}

func (t *BlockSquareTransform) String() string {
	return "block_square_transform"
}

func (t *BlockSquareTransform) Transform(readOnlyBlk *pbbstream.Block, in transform.Input) (transform.Output, error) {
	if !t.validateInput(in) {
		return nil, fmt.Errorf("invalid input type %q", in.Type())
	}

	return &BlockNumberSquareOutput{
		Square: readOnlyBlk.Number * readOnlyBlk.Number,
	}, nil
}
