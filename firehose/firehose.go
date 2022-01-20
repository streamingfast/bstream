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

func WithPreprocessFunc(pp bstream.PreprocessFunc) Option {
	return func(f *Firehose) {
		f.preprocessFunc = pp
	}
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

func WithStopBlock(stopBlockNum uint64) Option { //inclusive
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

func (f *Firehose) forkableHandlerWrapper(cursor *bstream.Cursor, libInclusive bool) func(h bstream.Handler, lib bstream.BlockRef) bstream.Handler {
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

func (f *Firehose) getIrreversibleIndex(ctx context.Context, startBlockNum uint64) (irrIndex *bstream.IrrBlocksIndex, err error) {
	if f.irreversibleBlocksIndexStore != nil {
		var cursorLIB bstream.BlockRef
		var startBlockNum uint64

		if f.cursor.IsEmpty() {
		} else {
			cursorLIB = f.cursor.LIB
			startBlockNum = f.cursor.Block.Num()
		}
		irrIndex = bstream.NewIrreversibleBlocksIndex(f.irreversibleBlocksIndexStore, f.irreversibleBlocksIndexBundles, startBlockNum, cursorLIB)
	}
	return
}

func (f *Firehose) joiningSourceFactoryFromTracker(ctx context.Context, tracker *bstream.Tracker) bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {

		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(f.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}

		if f.liveHeadTracker != nil {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, f.liveHeadTracker))
		}

		f.logger.Info("resolving start block", zap.Uint64("start_block", startBlockNum))
		fileStartBlock, previousIrreversibleID, err := f.tracker.ResolveStartBlock(ctx, startBlockNum)
		if err != nil {
			return nil // , fmt.Errorf("failed to resolve start block: %w", err)
		}
		f.logger.Info("firehose pipeline bootstrapping from tracker",
			zap.Uint64("requested_start_block", startBlockNum),
			zap.Uint64("file_start_block", fileStartBlock),
			zap.String("previous_irr_id", previousIrreversibleID),
		)

		if previousIrreversibleID != "" {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(previousIrreversibleID))
		}

		cappedHandler := bstream.NewMinimalBlockNumFilter(startBlockNum, h)

		return bstream.NewJoiningSource(f.fileSourceFactory(fileStartBlock), f.liveSourceFactory, cappedHandler, joiningSourceOptions...)

	}
}

func (f *Firehose) joiningSourceFactory() bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(f.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}
		cappedHandler := bstream.NewMinimalBlockNumFilter(startBlockNum, h)
		f.logger.Info("firehose pipeline bootstrapping",
			zap.Int64("start_block", f.startBlockNum),
		)
		return bstream.NewJoiningSource(f.fileSourceFactory(startBlockNum), f.liveSourceFactory, cappedHandler, joiningSourceOptions...)
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

		fileStartBlock := f.cursor.LIB.Num() // we don't use startBlockNum, the forkable will wait for the cursor before it forwards blocks
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(f.cursor.LIB.ID()))

		f.logger.Info("firehose pipeline bootstrapping from cursor",
			zap.Uint64("file_start_block", fileStartBlock),
		)
		return bstream.NewJoiningSource(f.fileSourceFactory(fileStartBlock), f.liveSourceFactory, h, joiningSourceOptions...)
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

func (f *Firehose) createIndexedFileSource(ctx context.Context, irrIndex bstream.BlockIndex) (bstream.Source, error) {

	return bstream.NewIndexedFileSource(
		f.wrappedHandler(false),
		f.preprocessFunc,
		irrIndex,
		f.blocksStores[0], //FIXME
		f.joiningSourceFactory(),
		f.forkableHandlerWrapper(nil, false),
		f.logger,
	), nil
}

func (f *Firehose) createSource(ctx context.Context) (bstream.Source, error) {
	f.logger.Debug("setting up firehose pipeline")

	startBlockNum, err := resolveNegativeStartBlock(ctx, f.startBlockNum, f.tracker)
	if err != nil {
		return nil, err
	}

	if !f.cursor.IsEmpty() {
		startBlockNum = f.cursor.Block.Num()
	}

	if f.stopBlockNum > 0 && startBlockNum > f.stopBlockNum {
		return nil, NewErrInvalidArg("start block %d is after stop block %d", f.startBlockNum, f.stopBlockNum)
	}

	irrIndex, err := f.getIrreversibleIndex(ctx, startBlockNum)
	if err != nil {
		return nil, err
	}

	if irrIndex != nil {
		return f.createIndexedFileSource(ctx, irrIndex)
	}

	h := f.wrappedHandler(f.irreversibleBlocksIndexWritable)

	if f.cursor.IsEmpty() {
		forkableHandlerWrapper := f.forkableHandlerWrapper(nil, true)
		resolvedStartBlock, previousIrreversibleID, err := f.tracker.ResolveStartBlock(ctx, startBlockNum)
		if err != nil {
			return nil, err
		}

		var irrRef bstream.BlockRef
		if previousIrreversibleID != "" {
			irrRef = bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
		}
		forkableHandler := forkableHandlerWrapper(h, irrRef)
		jsf := f.joiningSourceFactoryFromTracker(ctx, f.tracker)
		return jsf(startBlockNum, forkableHandler), nil
	}

	forkableHandlerWrapper := f.forkableHandlerWrapper(f.cursor, true)
	forkableHandler := forkableHandlerWrapper(h, f.cursor.LIB)
	jsf := f.joiningSourceFactoryFromCursor(f.cursor)

	return jsf(startBlockNum, forkableHandler), nil

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
