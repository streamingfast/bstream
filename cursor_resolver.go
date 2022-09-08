package bstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

// cursorResolver is a handler that feeds from a source of new+irreversible blocks (filesource)
// and keeps blocks in a slice until cursor is passed.
// when it sees the cursor, it sends whatever is needed to bring the consumer back to a "new and irreversible" head
type cursorResolver struct {
	forkedBlocksStore dstore.Store

	handler Handler
	cursor  *Cursor
	logger  *zap.Logger

	mergedBlocksSeen []*BlockWithObj
	passthrough      bool
}

func newCursorResolverHandler(
	forkedBlocksStore dstore.Store,
	cursor *Cursor,
	h Handler,
	logger *zap.Logger) *cursorResolver {
	return &cursorResolver{
		forkedBlocksStore: forkedBlocksStore,
		cursor:            cursor,
		logger:            logger,
		handler:           h,
	}
}

func (f *cursorResolver) ProcessBlock(blk *Block, obj interface{}) error {
	if f.passthrough {
		return f.handler.ProcessBlock(blk, obj)
	}

	if blk.Number < f.cursor.Block.Num() {
		f.mergedBlocksSeen = append(f.mergedBlocksSeen, &BlockWithObj{blk, obj})
		return nil
	}

	ctx := context.Background()
	if blk.Number == f.cursor.Block.Num() {
		f.mergedBlocksSeen = append(f.mergedBlocksSeen, &BlockWithObj{blk, obj})
		if blk.Id == f.cursor.Block.ID() {
			if err := f.sendMergedBlocksBetween(StepFinal, f.cursor.LIB.Num(), f.cursor.Block.Num()); err != nil {
				return err
			}
			f.passthrough = true
			return nil
		}
		// we are on a fork
		undoBlocks, continueAfter, err := f.resolve(ctx)
		if err != nil {
			return err
		}

		if err := f.sendUndoBlocks(undoBlocks); err != nil {
			return err
		}
		if err := f.sendMergedBlocksBetween(StepFinal, f.cursor.LIB.Num(), continueAfter); err != nil {
			return err
		}
		if err := f.sendMergedBlocksBetween(StepNewFinal, continueAfter, blk.Number); err != nil {
			return err
		}

		f.passthrough = true

		return nil
	}

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

	reader := bytes.NewReader(data)
	blockReader, err := GetBlockReaderFactory.New(reader)
	if err != nil {
		return nil, fmt.Errorf("unable to create block reader: %w", err)
	}
	blk, err := blockReader.Read()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("block reader failed: %w", err)
	}
	return blk, nil
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
			return nil, 0, fmt.Errorf("cannot resolve cursor pointing to block %s: missing link: no one-block-file or merged block found with ID %s", block, previousID)
		}

		if forkedBlock.Num < lib.Num() {
			return nil, 0, fmt.Errorf("cannot resolve cursor pointing to block %s: missing link: forked chain goes beyond LIB, looking for ID %s (this should not happens)", block, previousID)
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
