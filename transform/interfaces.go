package transform

import "github.com/streamingfast/bstream"

type Transform interface {
	Doc() string
	// The SolanFilter must be the first filter in the list
	// It outputs a solana.Block
	// It must have no input transforms

	// EVENTUALLY, if we need to dynamically negotiate things
	// InPorts() []string
	// OutPorts() []string

	// PLUS TARD MERCI
	// Hash() string
}

type BlockTransformer interface {
	// ALWAYS PARALLELIZABLE BY DEFINITION. It is CONTEXT FREE with regards to other blocks around him.
	Transform(blk *bstream.Block, in Input) (out Output)

	// ModifiesBlock bool
	// IsDeterministic bool
}

type BlockRangeTransformer interface {
	CanSkipRange(start, end uint64) (bool, error)
	NextUnsparse(block uint64) uint64
}
