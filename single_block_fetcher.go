package bstream

import (
	"context"
	"fmt"
	"strings"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

func FetchBlockFromOneBlockStore(
	ctx context.Context,
	num uint64,
	id string,
	store dstore.Store,
) (*pbbstream.Block, error) {
	if obfs, err := listOneBlocks(ctx, num, num+1, store); err == nil {
		canonicalID := NormalizeBlockID(id)
		for _, obf := range obfs {
			if strings.HasSuffix(canonicalID, obf.ID) {
				data, err := obf.Data(ctx, OneBlockDownloaderFromStore(store))
				if err != nil {
					return nil, err
				}
				return decodeOneblockfileData(data)
			}
		}
	}
	return nil, dstore.ErrNotFound
}

func FetchBlockMetaFromOneBlockStore(
	ctx context.Context,
	num uint64,
	id string,
	store dstore.Store,
) (*pbbstream.BlockMeta, error) {
	if obfs, err := listOneBlocks(ctx, num, num+1, store); err == nil {
		canonicalID := NormalizeBlockID(id)
		for _, obf := range obfs {
			if strings.HasSuffix(canonicalID, obf.ID) {
				data, err := obf.Data(ctx, OneBlockDownloaderFromStore(store))
				if err != nil {
					return nil, err
				}
				return decodeOneblockfileToBlockMeta(data)
			}
		}
	}
	return nil, dstore.ErrNotFound
}

// FetchBlockMetaByHashFromOneBlockStore fetches a block meta by its hash from a single block store.
// It will list all the blocks in the store and find the one that matches the hash. If the
// block is not found, it returns `nil, nil`.
func FetchBlockMetaByHashFromOneBlockStore(
	ctx context.Context,
	id string,
	store dstore.Store,
) (*pbbstream.BlockMeta, error) {
	canonicalID := NormalizeBlockID(id)
	isBlockHash := func(file *OneBlockFile) bool {
		return NormalizeBlockID(file.ID) == canonicalID
	}

	obf, err := findOneBlockFile(ctx, store, isBlockHash)
	if err != nil {
		return nil, fmt.Errorf("find one block file: %w", err)
	}

	if obf == nil {
		return nil, nil
	}

	data, err := obf.Data(ctx, OneBlockDownloaderFromStore(store))
	if err != nil {
		return nil, fmt.Errorf("download one block data: %w", err)
	}

	return decodeOneblockfileToBlockMeta(data)
}

func FetchBlockFromMergedBlocksStore(
	ctx context.Context,
	num uint64,
	store dstore.Store,
) (*pbbstream.Block, error) {
	var foundBlock *pbbstream.Block
	h := HandlerFunc(func(blk *pbbstream.Block, _ interface{}) error {
		if blk.Number < num {
			return nil
		}
		if blk.Number > num {
			return dstore.StopIteration
		}
		foundBlock = blk
		return nil
	})
	fs := NewFileSource(
		store,
		num,
		h,
		zap.NewNop(),
		FileSourceWithStopBlock(num),
	)
	fs.Run()
	<-fs.Terminated()
	if foundBlock != nil {
		return foundBlock, nil
	}

	return nil, dstore.ErrNotFound
}
