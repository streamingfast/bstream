package bstream

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

var ErrResolveCursor = errors.New("cannot resolve cursor")

// cursorResolver is a handler that feeds from a source of new+irreversible blocks (filesource)
// and keeps blocks in a slice until cursor is passed.
// when it sees the cursor, it sends whatever is needed to bring the consumer back to a "new and irreversible" head
type cursorResolver struct {
	forkedBlocksStore dstore.Store

	handler Handler
	cursor  *Cursor
	logger  *zap.Logger

	passThroughCursor bool

	mergedBlocksSeen []*BlockWithObj
	resolved         bool
}

func newCursorResolverHandler(
	forkedBlocksStore dstore.Store,
	cursor *Cursor,
	passThroughCursor bool,
	h Handler,
	logger *zap.Logger) *cursorResolver {
	return &cursorResolver{
		forkedBlocksStore: forkedBlocksStore,
		passThroughCursor: passThroughCursor,
		cursor:            cursor,
		logger:            logger,
		handler:           h,
	}
}

func (f *cursorResolver) ProcessBlock(blk *Block, obj interface{}) error {
	if f.resolved {
		return f.handler.ProcessBlock(blk, obj)
	}

	if f.passThroughCursor && blk.Num() <= f.cursor.LIB.Num() {
		// in passThroughMode, we send everything up to LIB
		// then we start accumulating until we reach the cursor block
		return f.handler.ProcessBlock(blk, obj)
	}

	if blk.Number < f.cursor.Block.Num() {
		f.mergedBlocksSeen = append(f.mergedBlocksSeen, &BlockWithObj{blk, obj})
		return nil
	}

	f.mergedBlocksSeen = append(f.mergedBlocksSeen, &BlockWithObj{blk, obj})
	if blk.Id == f.cursor.Block.ID() {
		f.resolved = true
		if f.passThroughCursor {
			return f.sendMergedBlocksBetween(StepNewIrreversible, f.cursor.LIB.Num(), f.cursor.Block.Num())
		}

		if f.cursor.Step.Matches(StepUndo) {
			if f.cursor.Block.Num() > 0 {
				if err := f.sendMergedBlocksBetween(StepIrreversible, f.cursor.LIB.Num(), f.cursor.Block.Num()-1); err != nil {
					return err
				}
			}
			return f.handler.ProcessBlock(blk, obj)
		}
		return f.sendMergedBlocksBetween(StepIrreversible, f.cursor.LIB.Num(), f.cursor.Block.Num())
	}

	if f.passThroughCursor {
		return fmt.Errorf("cannot resolve 'old cursor' from files in passthrough mode -- not implemented")
	}

	// we are on a fork
	ctx := context.Background()
	undoBlocks, continueAfter, err := f.resolve(ctx)
	if err != nil {
		return err
	}

	if err := f.sendUndoBlocks(undoBlocks); err != nil {
		return err
	}
	if err := f.sendMergedBlocksBetween(StepIrreversible, f.cursor.LIB.Num(), continueAfter); err != nil {
		return err
	}
	if err := f.sendMergedBlocksBetween(StepNewIrreversible, continueAfter, blk.Number); err != nil {
		return err
	}

	f.resolved = true

	return nil

}

func (f *cursorResolver) sendUndoBlocks(undoBlocks []*Block) error {
	for _, blk := range undoBlocks {
		block := blk.AsRef()
		obj := &wrappedObject{
			cursor: &Cursor{
				Step:      StepUndo,
				Block:     block,
				LIB:       f.cursor.LIB,
				HeadBlock: block, // FIXME
			}}
		if err := f.handler.ProcessBlock(blk, obj); err != nil {
			return err
		}
	}
	return nil
}

func (f *cursorResolver) sendMergedBlocksBetween(step StepType, exclusiveLow, inclusiveHigh uint64) error {
	for _, blockObj := range f.mergedBlocksSeen {
		if blockObj.Block.Number <= exclusiveLow {
			continue
		}
		if blockObj.Block.Number <= inclusiveHigh {
			obj := blockObj.Obj.(*wrappedObject)
			obj.cursor.Step = step
			if err := f.handler.ProcessBlock(blockObj.Block, obj); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *cursorResolver) oneBlocks(ctx context.Context, from, upTo uint64) (out map[string]*OneBlockFile, err error) {
	out = make(map[string]*OneBlockFile)

	fromStr := fmt.Sprintf("%010d", from)
	err = f.forkedBlocksStore.WalkFrom(ctx, "", fromStr, func(filename string) error {
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

func (f *cursorResolver) download(ctx context.Context, file *OneBlockFile) (*Block, error) {
	data, err := file.Data(ctx, OneBlockDownloaderFromStore(f.forkedBlocksStore))
	if err != nil {
		return nil, err
	}
	return decodeOneblockfileData(data)
}

func (f *cursorResolver) seenIrreversible(id string) *BlockWithObj {
	for _, blkObj := range f.mergedBlocksSeen {
		if strings.HasSuffix(blkObj.Block.Id, id) {
			return blkObj
		}
	}
	return nil
}

func (f *cursorResolver) resolve(ctx context.Context) (undoBlocks []*Block, continueAfter uint64, err error) {
	block := f.cursor.Block
	lib := f.cursor.LIB
	step := f.cursor.Step
	previousID := TruncateBlockID(block.ID())
	oneBlocks, err := f.oneBlocks(ctx, lib.Num(), block.Num())
	if err != nil {
		return nil, 0, err
	}

	for {
		if blkObj := f.seenIrreversible(previousID); blkObj != nil {
			continueAfter = blkObj.Block.Num()
			break
		}

		forkedBlock := oneBlocks[previousID]
		if forkedBlock == nil {
			return nil, 0, fmt.Errorf("%w: missing link between blocks %d and %s: no forked-block file found with ID ending with %s.", ErrResolveCursor, lib.Num(), block, previousID)
		}

		if forkedBlock.Num < lib.Num() {
			return nil, 0, fmt.Errorf("%w: block %s not linkable to canonical chain above final block %d (looking for ID ending with %s)", ErrResolveCursor, block, lib.Num(), previousID)
		}

		previousID = forkedBlock.PreviousID

		if forkedBlock.Num == block.Num() && step == StepUndo {
			// cursor block is already 'undone' for customer
			continue
		}

		fullBlk, err := f.download(ctx, forkedBlock)
		if err != nil {
			return nil, 0, fmt.Errorf("downloading one-block-file: %w", err)
		}
		undoBlocks = append(undoBlocks, fullBlk)

	}

	return
}
