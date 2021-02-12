package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
)

type ErrInvalidArg struct {
	message string
}

func NewErrInvalidArg(m string, args ...interface{}) *ErrInvalidArg {
	return &ErrInvalidArg{
		message: fmt.Sprintf(m, args...),
	}
}

func (e *ErrInvalidArg) Error() string {
	return e.message
}

var ErrStopBlockReached = errors.New("stop block reached")

type Option = func(s *Firehose)

type Firehose struct {
	liveSourceFactory       bstream.SourceFactory
	blocksStores            []dstore.Store
	startBlockNum           int64
	stopBlockNum            uint64
	burst                   uint64
	handler                 bstream.Handler
	cursor                  *forkable.Cursor
	forkSteps               forkable.StepType
	tracker                 *bstream.Tracker
	preprocessFunc          bstream.PreprocessFunc
	liveHeadTracker         bstream.BlockRefGetter
	logger                  *zap.Logger
	preprocessorThreadCount int
}

func New(
	blocksStores []dstore.Store,
	startBlockNum int64,
	handler bstream.Handler,
	options ...Option) *Firehose {
	f := &Firehose{
		blocksStores:  blocksStores,
		startBlockNum: startBlockNum,
		logger:        zlog,
		forkSteps:     forkable.StepsAll,
		handler:       handler,
	}

	for _, option := range options {
		option(f)

	}
	return f
}

func WithConcurrentPreprocessor(threadCount int) Option {
	return func(f *Firehose) {
		f.preprocessorThreadCount = threadCount
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(f *Firehose) {
		f.logger = logger
	}
}

func WithForkableSteps(steps forkable.StepType) Option {
	return func(f *Firehose) {
		f.forkSteps = steps
	}
}

func WithCursor(cursor *forkable.Cursor) Option {
	return func(f *Firehose) {
		f.cursor = cursor
	}
}

func WithPreproc(preprocessFunc bstream.PreprocessFunc) Option {
	return func(f *Firehose) {
		f.preprocessFunc = preprocessFunc
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

func WithStopBlock(stopBlockNum uint64) Option {
	return func(f *Firehose) {
		f.stopBlockNum = stopBlockNum
	}
}

func WithLiveSource(liveSourceFactory bstream.SourceFactory) Option {
	return func(f *Firehose) {
		f.liveSourceFactory = liveSourceFactory
	}
}

func (f *Firehose) setupPipeline(ctx context.Context) (bstream.Source, error) {
	f.logger.Debug("setting up firehose pipeline")
	stopNow := func(blockNum uint64) bool {
		return f.stopBlockNum > 0 && blockNum > f.stopBlockNum
	}

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if stopNow(block.Number) {
			return ErrStopBlockReached
		}

		return f.handler.ProcessBlock(block, obj)
	})

	var startBlock uint64
	var handler bstream.Handler
	var err error
	var fileStartBlock uint64

	joiningSourceOptions := []bstream.JoiningSourceOption{
		// First so JoiningSource can use it when dealing with other options
		bstream.JoiningSourceLogger(f.logger),
		// Overrides default behavior when targetBlockID is set, used as side effect of EternalSource
		bstream.JoiningSourceStartLiveImmediately(false),
	}

	forkableOptions := []forkable.Option{
		forkable.WithLogger(f.logger),
		forkable.WithFilters(f.forkSteps),
	}

	if f.cursor != nil {
		startBlock = f.cursor.StartBlockNum()
		fileStartBlock = startBlock
		f.logger.Info("firehose pipeline bootstrapping from cursor",
			zap.Uint64("start_block_num", startBlock),
			zap.Uint64("file_start_block", fileStartBlock),
		)
		forkableOptions = append(forkableOptions, forkable.FromCursor(f.cursor))
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(f.cursor.LIB.ID()))
		handler = handlerFunc
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

		if previousIrreversibleID != "" {
			irrRef := bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
			f.logger.Debug("configuring inclusive LIB on forkable handler", zap.Stringer("irr_ref", irrRef))
			forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(irrRef))
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(previousIrreversibleID))
		}

		fileStartBlock = resolvedStartBlock
		handler = bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, handlerFunc, bstream.GateOptionWithLogger(f.logger))

		f.logger.Info("firehose pipeline bootstrapping from tracker",
			zap.Int64("requested_start_block", f.startBlockNum),
			zap.Uint64("resolved_start_block", startBlock),
			zap.Uint64("file_start_block", fileStartBlock),
			zap.String("previous_irr_id", previousIrreversibleID),
		)
	} else {
		if f.startBlockNum < 0 {
			return nil, NewErrInvalidArg("start block %d cannot be relative without a tracker", f.startBlockNum)
		}

		// no tracker, no cursor and positive start block num
		fileStartBlock = uint64(f.startBlockNum)
		startBlock = uint64(f.startBlockNum)
		handler = bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, handlerFunc, bstream.GateOptionWithLogger(f.logger))

		f.logger.Info("firehose pipeline bootstrapping",
			zap.Int64("start_block", f.startBlockNum),
			zap.Uint64("file_start_block", fileStartBlock),
		)
	}

	forkHandler := forkable.New(handler, forkableOptions...)

	var liveSourceFactory bstream.SourceFactory
	if f.liveSourceFactory != nil {
		liveSourceFactory = func(subHandler bstream.Handler) bstream.Source {
			if f.preprocessFunc != nil {
				// clone it before passing it to filtering processors, so it doesn't mutate
				// the subscriptionHub's Blocks and affects other subscribers to the hub.
				subHandler = bstream.CloneBlock(bstream.NewPreprocessor(f.preprocessFunc, subHandler))
			}

			return f.liveSourceFactory(subHandler)
		}
	}

	var fileSourceOptions []bstream.FileSourceOption
	if len(f.blocksStores) > 1 {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(f.blocksStores[1:]))
	}
	fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(f.preprocessorThreadCount))

	fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			f.blocksStores[0],
			fileStartBlock,
			1,
			f.preprocessFunc,
			subHandler,
			fileSourceOptions...,
		)
		return fs
	})

	if f.liveHeadTracker != nil {
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, f.liveHeadTracker))
	}

	js := bstream.NewJoiningSource(fileSourceFactory, liveSourceFactory, forkHandler, joiningSourceOptions...)

	f.logger.Info("starting stream blocks",
		zap.Stringer("cursor", f.cursor),
		zap.Int64("start_block", f.startBlockNum),
		zap.Uint64("absolute_start_block", startBlock),
	)

	return js, nil
}

func (f *Firehose) Run(ctx context.Context) error {
	source, err := f.setupPipeline(ctx)
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
		return err
	}
	return nil
}