package transform

import (
	"fmt"
	"strings"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// PassthroughFromTransforms returns a PassthroughTransform 'anyTransforms' refers to a single transform of that kind
func (r *Registry) PassthroughFromTransforms(anyTransforms []*anypb.Any) (PassthroughTransform, error) {
	if len(anyTransforms) != 1 {
		return nil, nil
	}
	t, err := r.New(anyTransforms[0])
	if err != nil {
		return nil, fmt.Errorf("unable to instantiate transform: %w", err)
	}
	if pr, ok := t.(PassthroughTransform); ok {
		return pr, nil
	}
	return nil, nil
}

// BuildFromTransforms returns a PreprocessFunc, an optional BlockIndexProvider, a human-readable description and an error
// It will fail if it receives a transform of type Passthrough or a transform that does not match any interface
func (r *Registry) BuildFromTransforms(anyTransforms []*anypb.Any) (
	bstream.PreprocessFunc,
	bstream.BlockIndexProvider,
	string,
	error,
) {
	if len(anyTransforms) == 0 {
		return nil, nil, "", nil
	}
	var blockIndexProvider bstream.BlockIndexProvider
	var descs []string
	var ppTransforms []PreprocessTransform

	for _, transform := range anyTransforms {
		t, err := r.New(transform)
		if err != nil {
			return nil, nil, "", fmt.Errorf("unable to instantiate transform: %w", err)
		}
		if _, ok := t.(PassthroughTransform); ok {
			return nil, nil, "", fmt.Errorf("cannot build preprocessor func from 'Passthrough' type of transform")
		}

		descs = append(descs, t.String())

		var matches bool
		if pp, ok := t.(PreprocessTransform); ok {
			matches = true
			ppTransforms = append(ppTransforms, pp)
		}

		if bipg, ok := t.(bstream.BlockIndexProviderGetter); ok {
			matches = true
			if blockIndexProvider != nil { // TODO eventually, should we support multiple indexes ?
				zlog.Warn("multiple index providers from transform, ignoring this one", zap.Stringer("transform", t))
			} else {
				blockIndexProvider = bipg.GetIndexProvider()
			}
		}
		if !matches {
			return nil, nil, "", fmt.Errorf("transform %s does not match any transform interface", transform.String())
		}
	}

	descriptions := strings.Join(descs, ",")

	var in Input
	preprocessFunc := func(blk *pbbstream.Block) (interface{}, error) {

		in = NewNilObj()
		var out proto.Message
		var err error
		for idx, transform := range ppTransforms {
			if out, err = transform.Transform(blk, in); err != nil {
				return nil, fmt.Errorf("transform %d failed: %w", idx, err)
			}
			in = &InputObj{
				_type: string(proto.MessageName(out)),
				obj:   out,
			}
		}
		return out, nil
	}
	return preprocessFunc, blockIndexProvider, descriptions, nil
}
