package transform

import (
	"fmt"
	"strings"

	"github.com/streamingfast/bstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// BuildFromTransforms returns
// One or many of those 3:
//   * a PreprocessFunc,
//   * a LinearPreprocessFunc
// type LinearPreprocessFunc func(blk *Block, step StepType) (interface{}, error)
//   * a BlockIndexProvider (for skipping blocks),
// And those:
//   * a human-readable description
//   * a error when invalid or incompatible transforms are requested
func (r *Registry) BuildFromTransforms(anyTransforms []*anypb.Any) (bstream.PreprocessFunc, bstream.LinearPreprocessFunc, bstream.BlockIndexProvider, string, error) {
	var blockIndexProvider bstream.BlockIndexProvider
	allTransforms := []Transform{}
	linearTransforms := []LinearTransform{}
	parallelTransforms := []ParallelTransform{}
	for _, transform := range anyTransforms {
		t, err := r.New(transform)
		if err != nil {
			return nil, nil, nil, "", fmt.Errorf("unable to instantiate transform: %w", err)
		}
		if lt, ok := t.(LinearTransform); ok {
			linearTransforms = append(linearTransforms, lt)
		}
		if pt, ok := t.(ParallelTransform); ok {
			parallelTransforms = append(parallelTransforms, pt)
		}
		allTransforms = append(allTransforms, t)
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
	for _, t := range allTransforms {
		desc := fmt.Sprintf("%T", t)
		if st, ok := t.(fmt.Stringer); ok {
			desc = st.String()
		}
		descs = append(descs, desc)
	}
	descriptions := strings.Join(descs, ",")

	var in Input
	var parallelPreprocFunc bstream.PreprocessFunc
	if len(parallelTransforms) != 0 {
		parallelPreprocFunc = func(blk *bstream.Block) (interface{}, error) {
			clonedBlk := blk.Clone()
			in = NewNilObj()
			var out proto.Message
			var err error
			for idx, transform := range parallelTransforms {
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
	}

	var linearPreprocFunc bstream.LinearPreprocessFunc
	if len(linearTransforms) > 1 {
		return nil, nil, nil, "", fmt.Errorf("there cannot be more than one linear transform")
	}
	if len(linearTransforms) == 1 {
		// type LinearPreprocessFunc func(blk *Block, step StepType) (interface{}, error)
		linearPreprocFunc = func(blk *bstream.Block, step bstream.StepType) (interface{}, error) {
			return linearTransforms[0].Transform(blk, step)
		}
	}

	return parallelPreprocFunc, linearPreprocFunc, blockIndexProvider, descriptions, nil
}
