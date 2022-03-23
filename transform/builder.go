package transform

import (
	"fmt"
	"strings"

	"github.com/streamingfast/bstream"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Transformer struct {
	preprocFunc bstream.PreprocessFunc
	processFunc ProcessBlockFunc

	BlockIndexProvider bstream.BlockIndexProvider
	StartBlockResolver StartBlockResolver
	Description        string
}

func (t *Transformer) HasPreprocessor() bool {
	return t.preprocFunc != nil
}

func (t *Transformer) HasProcessor() bool {
	return t.processFunc != nil
}

func (t *Transformer) PreprocessBlock(readOnlyBlk *bstream.Block) (interface{}, error) {
	return t.preprocFunc(readOnlyBlk)
}

func (t *Transformer) ProcessBlock(readOnlyBlk *bstream.Block, step bstream.StepType) (interface{}, error) {
	return t.processFunc(readOnlyBlk, step)
}

func (r *Registry) Transformer(anyTransforms []*anypb.Any) (*Transformer, error) {
	var linearProcessFunc ProcessBlockFunc
	var parallelTransforms []ParallelTransform
	var blockIndexProvider bstream.BlockIndexProvider
	var startBlockResolver StartBlockResolver
	var descriptions []string

	for _, transform := range anyTransforms {
		t, err := r.New(transform)
		if err != nil {
			return nil, fmt.Errorf("unable to instantiate transform: %w", err)
		}

		if lt, ok := t.(LinearTransform); ok {
			if linearProcessFunc != nil {
				return nil, fmt.Errorf("cannot have more than one linear transform")
			}
			linearProcessFunc = lt.ProcessBlock
		}

		if sbr, ok := t.(TransformWithStartBlock); ok {
			if startBlockResolver != nil {
				return nil, fmt.Errorf("cannot have more than one start block resolver")
			}
			startBlockResolver = sbr.ResolveStartBlock
		}
		if pt, ok := t.(ParallelTransform); ok {
			parallelTransforms = append(parallelTransforms, pt)
		}

		if bipg, ok := t.(bstream.BlockIndexProviderGetter); ok {
			if blockIndexProvider != nil {
				zlog.Debug("multiple index providers from transform, ignoring")
			} else {
				blockIndexProvider = bipg.GetIndexProvider()
			}
		}

		desc := fmt.Sprintf("%T", t)
		if st, ok := t.(fmt.Stringer); ok {
			desc = st.String()
		}
		descriptions = append(descriptions, desc)
	}

	var parallelPreprocFunc bstream.PreprocessFunc
	if len(parallelTransforms) != 0 {
		parallelPreprocFunc = func(blk *bstream.Block) (interface{}, error) {
			clonedBlk := blk.Clone()
			var in Input = NewNilObj()
			var out proto.Message
			var err error
			for idx, transform := range parallelTransforms {
				if out, err = transform.PreprocessBlock(clonedBlk, in); err != nil {
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

	out := &Transformer{
		BlockIndexProvider: blockIndexProvider,
		StartBlockResolver: startBlockResolver,
		Description:        strings.Join(descriptions, ","),
		processFunc:        linearProcessFunc,
		preprocFunc:        parallelPreprocFunc,
	}

	out.preprocFunc = parallelPreprocFunc
	return out, nil
}
