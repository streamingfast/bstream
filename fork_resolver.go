package bstream

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/streamingfast/dstore"
)

type forkResolver struct {
	oneBlocksStore     dstore.Store
	mergedBlocksStore  dstore.Store
	blockReaderFactory BlockReaderFactory

	oneBlockDownloader OneBlockDownloaderFunc
}

func newForkResolver(oneBlocksStore dstore.Store,
	mergedBlocksStore dstore.Store,
	blockReaderFactory BlockReaderFactory,
) *forkResolver {

	return &forkResolver{
		oneBlocksStore:     oneBlocksStore,
		mergedBlocksStore:  mergedBlocksStore,
		blockReaderFactory: blockReaderFactory,
		oneBlockDownloader: OneBlockDownloaderFromStore(oneBlocksStore),
	}
}

func (f *forkResolver) oneBlocks(ctx context.Context, from, upTo uint64) (out map[string]*OneBlockFile, err error) {
	out = make(map[string]*OneBlockFile)

	fromStr := fmt.Sprintf("%010d", from)
	err = f.oneBlocksStore.WalkFrom(ctx, "", fromStr, func(filename string) error {
		obf, err := NewOneBlockFile(filename)
		if err != nil {
			// TODO: log skipping files
			return nil
		}
		out[obf.ID] = obf
		return nil
	})
	return
}

func (f *forkResolver) mergedBlockRefs(ctx context.Context, base uint64) (out map[string]BlockRef, err error) {
	filename := fmt.Sprintf("%010d", base)

	reader, err := f.mergedBlocksStore.OpenObject(context.Background(), filename)
	if err != nil {
		return nil, fmt.Errorf("fetching %s from block store: %w", filename, err)
	}

	blockReader, err := f.blockReaderFactory.New(reader)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("unable to create block reader: %w", err)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var blk *Block
		blk, err = blockReader.Read()
		if err != nil && err != io.EOF {
			return nil, err
		}

		if blk != nil {
			id := TruncateBlockID(blk.Id)
			out[id] = NewBlockRef(id, blk.Number)
		}
		if err == io.EOF {
			err = nil
			break
		}
	}

	return
}

func parseFilenames(in []string, upTo uint64) map[string]*OneBlockFile {
	out := make(map[string]*OneBlockFile)
	for _, f := range in {
		obf, err := NewOneBlockFile(f)
		if err != nil {
			continue
		}
		if obf.Num > upTo {
			continue
		}
		out[obf.ID] = obf
	}
	return out
}

func (f *forkResolver) download(ctx context.Context, file *OneBlockFile) (*Block, error) {
	data, err := file.Data(ctx, f.oneBlockDownloader)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(data)
	blockReader, err := f.blockReaderFactory.New(reader)
	if err != nil {
		return nil, fmt.Errorf("unable to create block reader: %w", err)
	}
	blk, err := blockReader.Read()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("block reader failed: %w", err)
	}
	return blk, nil
}

func (f *forkResolver) resolve(ctx context.Context, block BlockRef, lib BlockRef) (undoBlocks []BlockRef, continueAfter uint64, err error) {
	base := block.Num() / 100 * 100
	mergedBlocks, err := f.mergedBlockRefs(ctx, base)
	if err != nil {
		return nil, 0, err
	}
	nextID := TruncateBlockID(block.ID())
	oneBlocks, err := f.oneBlocks(ctx, lib.Num(), block.Num())
	if err != nil {
		return nil, 0, err
	}

	for {
		if blk := mergedBlocks[nextID]; blk != nil {
			continueAfter = blk.Num()
			break
		}

		forkedBlock := oneBlocks[nextID]

		if forkedBlock == nil && base <= lib.Num() {
			return nil, 0, fmt.Errorf("cannot resolve block %s: no oneBlockFile or merged block found with ID %s", block, nextID)
		}

		// also true when forkedBlock.Num < base && base <= lib.Num()
		if forkedBlock.Num < lib.Num() {
			return nil, 0, fmt.Errorf("cannot resolve block %s: forked chain goes beyond LIB, looking for ID %s (this should not happens)", block, nextID)
		}

		if forkedBlock == nil || forkedBlock.Num < base {
			base -= 100
			previousMergedBlocks, err := f.mergedBlockRefs(ctx, base)
			if err != nil {
				return nil, 0, fmt.Errorf("cannot resolve block %s (cannot load previous bundle (%d): %w)", block, base, err)
			}
			for k, v := range previousMergedBlocks {
				mergedBlocks[k] = v
			}
			continue // retry with more mergedBlocks loaded
		}

		fullBlk, err := f.download(ctx, forkedBlock)
		if err != nil {
			return nil, 0, fmt.Errorf("downloading one_block_file: %w", err)
		}

		undoBlocks = append(undoBlocks, fullBlk)
		nextID = forkedBlock.PreviousID
	}

	return
}
