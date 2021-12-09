package transform

import (
	"github.com/streamingfast/bstream"
	"google.golang.org/protobuf/types/known/anypb"
)

// Une fonction qui lis la request.BlockStreamV2, qui lookup le registry et retourne une erreur si yé pas là.. et build up une liste de `myTransforms`  pour passer ici-bas

// Une fonction qui prends la liste de mes transforms, pré-vetted, et qui build une PREPROC FUNC

func BuildFromTransforms(transforms []anypb.Any) bstream.PreprocessFunc {
	return func(blk *bstream.Block) (interface{}, error) {
		var in Input
		_ = in
		var output Output
		for _, transform := range transforms {
			_ = transform
		}
		return output, nil
	}
}
