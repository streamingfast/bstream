package transform

// Une fonction qui lis la request.BlockStreamV2, qui lookup le registry et retourne une erreur si yé pas là.. et build up une liste de `myTransforms`  pour passer ici-bas

// Une fonction qui prends la liste de mes transforms, pré-vetted, et qui build une PREPROC FUNC

//func BuildFrom(listOfTransforms []anypb.Any) bstream.ProcessingFunc {
//	return func(blk *Block) (interface{}, error) {
//		var in Input
//		var output Output
//		for transform := range myTransforms {
//			output = transform.Transform(prevVal)
//			prevVal = toInput(output)
//
//		}
//		return output, nil
//	}
//}
