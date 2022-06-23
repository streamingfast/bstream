package bstream

import (
	"fmt"
)

type resolver struct {
	// FIXME add context ...
	mergedBlockFilesGetter func(base uint64) (map[string]*OneBlockFile, error)
	oneBlockFilesGetter    func(upTo uint64) map[string]*OneBlockFile

	// func (s *DStoreIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*OneBlockFile, error) {
	// function to get the list... already have that somewhere in merger
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

func (r *resolver) download(file *OneBlockFile) BlockRef {
	return NewBlockRef(file.ID, file.Num) //FIXME need to add the block file downloader
}

func (r *resolver) loadPreviousMergedBlocks(base uint64, blocks map[string]*OneBlockFile) error {
	loadedBlocks, err := r.mergedBlockFilesGetter(base)
	if err != nil {
		return err
	}
	for k, v := range loadedBlocks {
		blocks[k] = v
	}
	return nil
}

func (r *resolver) resolve(block BlockRef, lib BlockRef) (undoBlocks []BlockRef, continueAfter uint64, err error) {
	base := block.Num() / 100 * 100
	mergedBlocks, err := r.mergedBlockFilesGetter(base)
	if err != nil {
		return nil, 0, err
	}
	nextID := TruncateBlockID(block.ID())
	oneBlocks := r.oneBlockFilesGetter(block.Num())

	for {
		if blk := mergedBlocks[nextID]; blk != nil {
			continueAfter = blk.Num
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
			err := r.loadPreviousMergedBlocks(base, mergedBlocks)
			if err != nil {
				return nil, 0, fmt.Errorf("cannot resolve block %s (cannot load previous bundle (%d): %w)", block, base, err)
			}
			continue // retry with more mergedBlocks loaded
		}

		undoBlocks = append(undoBlocks, r.download(forkedBlock))
		nextID = forkedBlock.PreviousID

	}

	return
}
