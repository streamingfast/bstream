package fetchers

import (
	"io"

	"github.com/streamingfast/dstore"
)

type BlockFetcher struct {
	blockNum uint64
	blockId  string
	store    dstore.Store
}

func NewBlockFetcher(blockNum uint64, blockId string) *BlockFetcher {
	return &BlockFetcher{
		blockNum: blockNum,
		blockId:  blockId,
	}
}

func (b BlockFetcher) Fetch(namespace string, key string) (io.ReadCloser, error) {
	//var fs *bstream.FileSource
	//var block *bstream.Block
	//
	////todo: This will log crap into the the cache.
	//handler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
	//	if blk.Num() != b.blockNum || blk.ID() != b.blockId {
	//		return nil
	//	}
	//
	//	block = blk
	//	fs.Shutdown(nil)
	//	return nil
	//})
	//
	//fs = bstream.NewFileSource(b.store, b.blockNum, 1, nil, handler)
	//fs.Run()
	//
	//if fs.Err() != nil {
	//	return nil, fs.Err()
	//}
	//
	////this so weird and feel so bad
	//data, found, err := block.Native.GetBytes()
	//if err != nil {
	//	return nil, fmt.Errorf("getting bytes from native: %w", err)
	//}
	//buf := bytes.NewBuffer()
	//buf.
	//
	//return block.Native

	return nil, nil
}
