package firehose

import (
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type Option = func(s *Firehose)

type Firehose struct {
	liveSourceFactory bstream.SourceFactory
	blocksStores      []dstore.Store

	startBlockNum                   int64
	stopBlockNum                    uint64
	irreversibleBlocksIndexStore    dstore.Store
	irreversibleBlocksIndexWritable bool
	irreversibleBlocksIndexBundles  []uint64

	handler bstream.Handler

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

func WithStreamBlocksParallelFiles(i int) Option {
	return func(f *Firehose) {
		f.streamBlocksParallelFiles = i
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(f *Firehose) {
		f.logger = logger
	}
}

func WithConfirmations(confirmations uint64) Option {
	return func(f *Firehose) {
		f.confirmations = confirmations
	}
}

func WithForkableSteps(steps bstream.StepType) Option {
	return func(f *Firehose) {
		f.forkSteps = steps
	}
}

func WithCursor(cursor *bstream.Cursor) Option {
	return func(f *Firehose) {
		f.cursor = cursor
	}
}

func WithLiveHeadTracker(liveHeadTracker bstream.BlockRefGetter) Option {
	return func(f *Firehose) {
		f.liveHeadTracker = liveHeadTracker
	}
}

func WithTracker(tracker *bstream.Tracker) Option {
	return func(f *Firehose) {
		f.tracker = tracker
	}
}

// Stop Block will be INCLUDED
func WithStopBlock(stopBlockNum uint64) Option {
	return func(f *Firehose) {
		f.stopBlockNum = stopBlockNum
	}
}

func WithIrreversibleBlocksIndex(store dstore.Store, readWrite bool, bundleSizes []uint64) Option {
	return func(f *Firehose) {
		f.irreversibleBlocksIndexStore = store
		f.irreversibleBlocksIndexWritable = readWrite
		f.irreversibleBlocksIndexBundles = bundleSizes
	}
}

func WithLiveSource(liveSourceFactory bstream.SourceFactory) Option {
	return func(f *Firehose) {
		f.liveSourceFactory = func(h bstream.Handler) bstream.Source {
			return liveSourceFactory(h)
		}
	}
}

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

func (f *Firehose) forkableHandlerFactory(cursor *bstream.Cursor, libInclusive bool) func(h bstream.Handler, lib bstream.BlockRef) bstream.Handler {
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

		return forkable.New(h, forkableOptions...)
	}
}

func resolveNegativeStartBlock(ctx context.Context, startBlockNum int64, tracker *bstream.Tracker) (uint64, error) {
	if startBlockNum > 0 {
		return uint64(startBlockNum), nil
	}
	out, err := tracker.GetRelativeBlock(ctx, startBlockNum, bstream.BlockStreamHeadTarget)
	if err != nil {
		return 0, fmt.Errorf("getting relative block: %w", err)
	}

	return out, nil
}

func (f *Firehose) getIrreversibleIndex(ctx context.Context) (irrIndex *bstream.IrrBlocksIndex, err error) {
	if f.irreversibleBlocksIndexStore != nil {
		var cursorLIB bstream.BlockRef
		var startBlockNum uint64

		if f.cursor.IsEmpty() {
			startBlockNum, err = resolveNegativeStartBlock(ctx, f.startBlockNum, f.tracker)
			if err != nil {
				return
			}
		} else {
			cursorLIB = f.cursor.LIB
			startBlockNum = f.cursor.Block.Num()
		}
		irrIndex = bstream.NewIrreversibleBlocksIndex(f.irreversibleBlocksIndexStore, f.irreversibleBlocksIndexBundles, startBlockNum, cursorLIB)
	}
	return
}

func (f *Firehose) createIndexedFileSource(ctx context.Context, irrIndex bstream.BlockIndex) (bstream.Source, error) {
	return bstream.NewIndexedFileSource(
		f.wrappedHandler(false),
	), nil
}

func (f *Firehose) createSource(ctx context.Context) (bstream.Source, error) {
	f.logger.Debug("setting up firehose pipeline")

	irrIndex, err := f.getIrreversibleIndex(ctx)
	if err != nil {
		return nil, err
	}

	if irrIndex != nil {
		return f.createIndexedFileSource(ctx, irrIndex)
	}

	//jsf := f.joiningSourceFactory()

	fileSourceFactory := bstream.SourceFromRefFactory(func(startBlockRef bstream.BlockRef, h bstream.Handler) bstream.Source {
		var fileSourceOptions []bstream.FileSourceOption
		if len(f.blocksStores) > 1 {
			fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(f.blocksStores[1:]))
		}
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(f.streamBlocksParallelFiles))

		if f.irreversibleBlocksIndexStore != nil {
			var unskippableBlockRef bstream.BlockRef
			if startBlockRef.ID() != "" {
				unskippableBlockRef = startBlockRef
			}

			// FIXME reimplement this
			_ = unskippableBlockRef

			//fileSourceOptions = append(fileSourceOptions,
			//	bstream.FileSourceWithSkipForkedBlocks(f.irreversibleBlocksIndexStore, f.irreversibleBlocksIndexBundles, unskippableBlockRef),
			//)
		}

		fs := bstream.NewFileSource(
			f.blocksStores[0],
			startBlockRef.Num(),
			f.streamBlocksParallelFiles,
			nil,
			h,
			fileSourceOptions...,
		)
		return fs
	})

	var handler bstream.Handler
	var fileStartBlock bstream.BlockRef

	joiningSourceOptions := []bstream.JoiningSourceOption{
		// First so JoiningSource can use it when dealing with other options
		bstream.JoiningSourceLogger(f.logger),
		// Overrides default behavior when targetBlockID is set, used as side effect of EternalSource
		bstream.JoiningSourceStartLiveImmediately(false),
	}

	if !f.cursor.IsEmpty() {
		startBlock = f.cursor.StartBlockNum()
		fileStartBlock = f.cursor.LIB
		f.logger.Info("firehose pipeline bootstrapping from cursor",
			zap.Uint64("start_block_num", startBlock),
			zap.Stringer("file_start_block", fileStartBlock),
		)
		forkableOptions = append(forkableOptions, forkable.FromCursor(f.cursor))
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(f.cursor.LIB.ID()))
		handler = f.wrappedHandler(f.irreversibleBlocksIndexWritable)
	} else if f.tracker != nil {

		f.logger.Info("resolving relative block", zap.Int64("start_block", f.startBlockNum))
		startBlock, err = f.tracker.GetRelativeBlock(ctx, f.startBlockNum, bstream.BlockStreamHeadTarget)
		if err != nil {
			return nil, fmt.Errorf("getting relative block: %w", err)
		}
		if f.stopBlockNum > 0 && startBlock > f.stopBlockNum {
			if f.startBlockNum < 0 {
				return nil, NewErrInvalidArg("resolved start block %d (relative %d) is after stop block %d", startBlock, f.startBlockNum, f.stopBlockNum)
			}
			return nil, NewErrInvalidArg("start block %d is after stop block %d", f.startBlockNum, f.stopBlockNum)
		}

		f.logger.Info("resolving start block", zap.Uint64("start_block", startBlock))
		resolvedStartBlock, previousIrreversibleID, err := f.tracker.ResolveStartBlock(ctx, startBlock)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve start block: %w", err)
		}

		// INCLUSIVE LIB, send it
		if previousIrreversibleID != "" {
			irrRef := bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(previousIrreversibleID))
		}

		fileStartBlock = bstream.NewBlockRef("", resolvedStartBlock)
		handler = bstream.NewMinimalBlockNumFilter(startBlock, f.wrappedHandler(f.irreversibleBlocksIndexWritable))

		f.logger.Info("firehose pipeline bootstrapping from tracker",
			zap.Int64("requested_start_block", f.startBlockNum),
			zap.Uint64("resolved_start_block", startBlock),
			zap.Stringer("file_start_block", fileStartBlock),
			zap.String("previous_irr_id", previousIrreversibleID),
		)
	} else {
		if f.startBlockNum < 0 {
			return nil, NewErrInvalidArg("start block %d cannot be relative without a tracker", f.startBlockNum)
		}

		// no tracker, no cursor and positive start block num

		startBlock = uint64(f.startBlockNum)
		fileStartBlock = bstream.NewBlockRef("", startBlock)
		handler = bstream.NewMinimalBlockNumFilter(startBlock, f.wrappedHandler(f.irreversibleBlocksIndexWritable))

		f.logger.Info("firehose pipeline bootstrapping",
			zap.Int64("start_block", f.startBlockNum),
			zap.Stringer("file_start_block", fileStartBlock),
		)
	}

	forkHandler := forkable.New(handler, forkableOptions...)

	numberedFileSourceFactory := bstream.SourceFactory(func(h bstream.Handler) bstream.Source {
		return fileSourceFactory(fileStartBlock, f.wrappedHandler(f.irreversibleBlocksIndexWritable))
	})

	if f.liveHeadTracker != nil {
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningLiveSourceWrapper(forkHandler))
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, f.liveHeadTracker))
	}

	js := bstream.NewJoiningSource(numberedFileSourceFactory, f.liveSourceFactory, forkHandler, joiningSourceOptions...)

	f.logger.Info("starting stream blocks",
		zap.Int64("start_block", f.startBlockNum),
		zap.Uint64("absolute_start_block", startBlock),
	)

	return js, nil
}

func (f *Firehose) Run(ctx context.Context) error {
	source, err := f.createSource(ctx)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-ctx.Done():
			zlog.Info("context is done. peer closed connection?")
			source.Shutdown(ctx.Err())
		}
	}()

	source.Run()
	if err := source.Err(); err != nil {
		zlog.Info("source shutting down", zap.Error(err))
		return err
	}
	return nil
}
