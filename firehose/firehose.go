package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type Firehose struct {
	liveSourceFactory bstream.SourceFactory
	blocksStores      []dstore.Store

	startBlockNum                   int64
	stopBlockNum                    uint64
	irreversibleBlocksIndexStore    dstore.Store
	irreversibleBlocksIndexWritable bool
	irreversibleBlocksIndexBundles  []uint64

	handler        bstream.Handler
	preprocessFunc bstream.PreprocessFunc

	cursor    *bstream.Cursor
	forkSteps bstream.StepType
	tracker   *bstream.Tracker

	liveHeadTracker           bstream.BlockRefGetter
	logger                    *zap.Logger
	confirmations             uint64
	streamBlocksParallelFiles int
}

// New creates a new Firehose instance configured using the provide options
func New(
	blocksStores []dstore.Store,
	startBlockNum int64,
	handler bstream.Handler,
	options ...Option) *Firehose {
	f := &Firehose{
		blocksStores:              blocksStores,
		startBlockNum:             startBlockNum,
		logger:                    zlog,
		forkSteps:                 bstream.StepsAll,
		handler:                   handler,
		streamBlocksParallelFiles: 1,
	}

	for _, option := range options {
		option(f)
	}

	return f
}

func (f *Firehose) Run(ctx context.Context) error {
	source, err := f.createSource(ctx)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-ctx.Done():
			source.Shutdown(ctx.Err())
		}
	}()

	source.Run()
	if err := source.Err(); err != nil {
		zlog.Debug("source shutting down", zap.Error(err))
		return err
	}
	return nil
}

func (f *Firehose) createSource(ctx context.Context) (bstream.Source, error) {
	f.logger.Debug("setting up firehose source")

	requestedStartBlockNum, startBlockNum, err := resolveStartBlock(ctx, f.startBlockNum, f.cursor, f.tracker)
	if err != nil {
		return nil, err
	}
	if f.stopBlockNum > 0 && startBlockNum > f.stopBlockNum {
		return nil, NewErrInvalidArg("start block %d is after stop block %d", startBlockNum, f.stopBlockNum)
	}

	hasCursor := !f.cursor.IsEmpty()

	if f.irreversibleBlocksIndexStore != nil {
		var cursorLIB bstream.BlockRef
		if hasCursor {
			cursorLIB = f.cursor.LIB
		}
		if irrIndex := bstream.NewIrreversibleBlocksIndex(f.irreversibleBlocksIndexStore, f.irreversibleBlocksIndexBundles, startBlockNum, cursorLIB); irrIndex != nil {
			return bstream.NewIndexedFileSource(
				f.wrappedHandler(false),
				f.preprocessFunc,
				irrIndex,
				f.blocksStores[0], //FIXME
				f.joiningSourceFactory(),
				f.forkableHandlerWrapper(nil, false, 0),
				f.logger,
				f.forkSteps,
			), nil
		}
	}

	// joiningSource -> forkable -> wrappedHandler
	h := f.wrappedHandler(f.irreversibleBlocksIndexWritable)
	if hasCursor {
		forkableHandlerWrapper := f.forkableHandlerWrapper(f.cursor, true, requestedStartBlockNum) // you don't want the cursor's block to be the lower limit
		forkableHandler := forkableHandlerWrapper(h, f.cursor.LIB)
		jsf := f.joiningSourceFactoryFromCursor(f.cursor)

		return jsf(startBlockNum, forkableHandler), nil
	}

	if f.tracker != nil {
		resolvedBlock, previousIrreversibleID, err := f.tracker.ResolveStartBlock(ctx, startBlockNum)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve start block: %w", err)
		}
		var irrRef bstream.BlockRef
		if previousIrreversibleID != "" {
			irrRef = bstream.NewBlockRef(previousIrreversibleID, resolvedBlock)
		}

		forkableHandlerWrapper := f.forkableHandlerWrapper(nil, true, startBlockNum)
		forkableHandler := forkableHandlerWrapper(h, irrRef)
		jsf := f.joiningSourceFactoryFromResolvedBlock(resolvedBlock, previousIrreversibleID)
		return jsf(startBlockNum, forkableHandler), nil
	}

	// no cursor, no tracker, probably just block files on disk
	forkableHandlerWrapper := f.forkableHandlerWrapper(nil, false, startBlockNum)
	forkableHandler := forkableHandlerWrapper(h, nil)
	jsf := f.joiningSourceFactory()
	return jsf(startBlockNum, forkableHandler), nil

}

// absoluteValue is either startBlockNum or HEAD + startBlockNum (for negative startBlockNum)
// effectiveValue == absoluteValue unless there is a cursor, the cursor has precedence
func resolveStartBlock(ctx context.Context, startBlockNum int64, cursor *bstream.Cursor, tracker *bstream.Tracker) (absoluteValue uint64, effectiveValue uint64, err error) {

	if startBlockNum < 0 {
		absoluteValue, err = tracker.GetRelativeBlock(ctx, startBlockNum, bstream.BlockStreamHeadTarget)
		if err != nil {
			if errors.Is(err, bstream.ErrGetterUndefined) {
				return 0, 0, NewErrInvalidArg("requested negative start block number (%d), but this instance has no HEAD tracker", startBlockNum)
			}
			return 0, 0, fmt.Errorf("getting relative block: %w", err)
		}
	} else {
		absoluteValue = uint64(startBlockNum)
	}

	if cursor.IsEmpty() {
		effectiveValue = absoluteValue
	} else {
		effectiveValue = cursor.Block.Num()
	}

	return
}

// adds stopBlock and irreversibleBlocksIndexer to the handler
func (f *Firehose) wrappedHandler(writeIrrBlkIdx bool) bstream.Handler {
	stopNow := func(blockNum uint64) bool {
		return f.stopBlockNum > 0 && blockNum > f.stopBlockNum
	}

	stoppingHandler := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if stopNow(block.Number) {
			return ErrStopBlockReached
		}
		return f.handler.ProcessBlock(block, obj)
	})

	if !writeIrrBlkIdx {
		return stoppingHandler
	}

	return forkable.NewIrreversibleBlocksIndexer(f.irreversibleBlocksIndexStore, f.irreversibleBlocksIndexBundles, stoppingHandler)
}

func (f *Firehose) forkableHandlerWrapper(cursor *bstream.Cursor, libInclusive bool, startBlockNum uint64) func(h bstream.Handler, lib bstream.BlockRef) bstream.Handler {
	return func(h bstream.Handler, lib bstream.BlockRef) bstream.Handler {

		forkableOptions := []forkable.Option{
			forkable.WithLogger(f.logger),
			forkable.WithFilters(f.forkSteps),
		}

		if f.confirmations != 0 {
			forkableOptions = append(forkableOptions,
				forkable.WithCustomLIBNumGetter(forkable.RelativeLIBNumGetter(f.confirmations)))
		}

		if !cursor.IsEmpty() {
			// does all the heavy lifting (setting the lib and start block, etc.)
			forkableOptions = append(forkableOptions, forkable.FromCursor(f.cursor))
		} else {
			if lib != nil {
				if libInclusive {
					f.logger.Debug("configuring inclusive LIB on forkable handler", zap.Stringer("lib", lib))
					forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(lib))
				} else {
					f.logger.Debug("configuring exclusive LIB on forkable handler", zap.Stringer("lib", lib))
					forkableOptions = append(forkableOptions, forkable.WithExclusiveLIB(lib))
				}
			}
		}

		return forkable.New(bstream.NewMinimalBlockNumFilter(startBlockNum, h), forkableOptions...)
	}
}

func (f *Firehose) joiningSourceFactoryFromResolvedBlock(fileStartBlock uint64, previousIrreversibleID string) bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {

		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(f.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}

		if f.liveHeadTracker != nil {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, f.liveHeadTracker))
		}

		f.logger.Info("firehose pipeline bootstrapping from tracker",
			zap.Uint64("requested_start_block", startBlockNum),
			zap.Uint64("file_start_block", fileStartBlock),
			zap.String("previous_irr_id", previousIrreversibleID),
		)

		if previousIrreversibleID != "" {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(previousIrreversibleID))
		}

		return bstream.NewJoiningSource(f.fileSourceFactory(fileStartBlock), f.liveSourceFactory, h, joiningSourceOptions...)

	}
}

func (f *Firehose) joiningSourceFactoryFromCursor(cursor *bstream.Cursor) bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {

		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(f.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}

		if f.liveHeadTracker != nil {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, f.liveHeadTracker))
		}

		fileStartBlock := cursor.LIB.Num() // we don't use startBlockNum, the forkable will wait for the cursor before it forwards blocks
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(cursor.LIB.ID()))

		f.logger.Info("firehose pipeline bootstrapping from cursor",
			zap.Uint64("file_start_block", fileStartBlock),
		)
		return bstream.NewJoiningSource(f.fileSourceFactory(fileStartBlock), f.liveSourceFactory, h, joiningSourceOptions...)
	}
}

func (f *Firehose) joiningSourceFactory() bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(f.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}
		f.logger.Info("firehose pipeline bootstrapping",
			zap.Int64("start_block", f.startBlockNum),
		)
		return bstream.NewJoiningSource(f.fileSourceFactory(startBlockNum), f.liveSourceFactory, h, joiningSourceOptions...)
	}
}

func (f *Firehose) fileSourceFactory(startBlockNum uint64) bstream.SourceFactory {
	return func(h bstream.Handler) bstream.Source {
		var fileSourceOptions []bstream.FileSourceOption
		if len(f.blocksStores) > 1 {
			fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(f.blocksStores[1:]))
		}
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(f.streamBlocksParallelFiles))

		fs := bstream.NewFileSource(
			f.blocksStores[0],
			startBlockNum,
			f.streamBlocksParallelFiles,
			f.preprocessFunc,
			h,
			fileSourceOptions...,
		)
		return fs
	}
}
