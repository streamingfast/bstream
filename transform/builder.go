package transform

import (
	"context"
	"fmt"
	"strings"

	"github.com/streamingfast/bstream"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type PassthroughTransform interface {
	String() string
	Run(ctx context.Context, req *pbfirehose.Request, output func(*bstream.Cursor, *anypb.Any) error)
}

// BuildFromTransforms returns a PreprocessFunc, an optional BlockIndexProvider, a human-readable description and an error
func (r *Registry) BuildFromTransforms(anyTransforms []*anypb.Any) (
	bstream.PreprocessFunc,
	bstream.BlockIndexProvider,
	PassthroughTransform,
	string,
	error,
) {
	var blockIndexProvider bstream.BlockIndexProvider
	transforms := []Transform{}
	for _, transform := range anyTransforms {
		t, err := r.New(transform)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("unable to instantiate transform: %w", err)
		}
		transforms = append(transforms, t)

		// passthrough runner currently highjacks all the other transforms
		if pr, ok := t.(PassthroughTransform); ok {
			if len(anyTransforms) != 1 {
				return nil, nil, nil, "", fmt.Errorf("transform %s is incompatible with any other transforms and must be used alone", pr.String())
			}
			return nil, nil, pr, pr.String(), nil
		}

		if bipg, ok := t.(bstream.BlockIndexProviderGetter); ok {
			if blockIndexProvider != nil { // TODO eventually, should we support multiple indexes ?
				zlog.Warn("multiple index providers from transform, ignoring")
			} else {
				zlog.Info("using index on transform")
				blockIndexProvider = bipg.GetIndexProvider()
			}
		}
	}

	var descs []string
	for _, t := range transforms {
		desc := fmt.Sprintf("%T", t)
		if st, ok := t.(fmt.Stringer); ok {
			desc = st.String()
		}
		descs = append(descs, desc)
	}
	descriptions := strings.Join(descs, ",")

	var in Input
	preprocessFunc := func(blk *bstream.Block) (interface{}, error) {
		clonedBlk := blk.Clone()
		in = NewNilObj()
		var out proto.Message
		var err error
		for idx, transform := range transforms {
			if out, err = transform.Transform(clonedBlk, in); err != nil {
				return nil, fmt.Errorf("transform %d failed: %w", idx, err)
			}
			in = &InputObj{
				_type: string(proto.MessageName(out)),
				obj:   out,
			}
		}
		return out, nil
	}
	return preprocessFunc, blockIndexProvider, nil, descriptions, nil
}
