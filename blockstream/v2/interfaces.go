package blockstream

import pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"

type BlockTrimmer interface {
	Trim(blk interface{}, details pbbstream.BlockDetails) interface{}
}

type BlockTrimmerFunc func(blk interface{}, details pbbstream.BlockDetails) interface{}

func (f BlockTrimmerFunc) Trim(blk interface{}, details pbbstream.BlockDetails) interface{} {
	return f(blk, details)
}
