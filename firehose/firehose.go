package firehose

import (
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"go.uber.org/zap"
)

type Option = func(s *Firehose)

type Firehose struct {
	liveSourceFactory bstream.SourceFactory
	fileSourceFactory bstream.SourceFromNumFactory

	startBlockNum int64
	stopBlockNum  uint64

	handler bstream.Handler

	cursor    *forkable.Cursor
	forkSteps forkable.StepType
	tracker   *bstream.Tracker

	liveHeadTracker bstream.BlockRefGetter
	logger          *zap.Logger
	confirmations   uint64
}

// New creates a new Firehose instance configured using the provide options
func New(
	fileSourceFactory bstream.SourceFromNumFactory,
	startBlockNum int64,
	handler bstream.Handler,
	options ...Option) *Firehose {
	f := &Firehose{
		fileSourceFactory: fileSourceFactory,
		startBlockNum:     startBlockNum,
		logger:            zlog,
		forkSteps:         forkable.StepsAll,
		handler:           handler,
	}

	for _, option := range options {
		option(f)

	}
	return f
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

func WithLiveSource(liveSourceFactory bstream.SourceFactory) Option {
	return func(f *Firehose) {
		f.liveSourceFactory = func(h bstream.Handler) bstream.Source {
			return liveSourceFactory(h)
		}
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

	if f.confirmations != 0 {
		forkableOptions = append(forkableOptions, forkable.WithCustomLIBNumGetter(forkable.RelativeLIBNumGetter(f.confirmations)))
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
		handler = bstream.NewMinimalBlockNumFilter(startBlock, handlerFunc)

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
		handler = bstream.NewMinimalBlockNumFilter(startBlock, handlerFunc)

		f.logger.Info("firehose pipeline bootstrapping",
			zap.Int64("start_block", f.startBlockNum),
			zap.Uint64("file_start_block", fileStartBlock),
		)
	}

	forkHandler := forkable.New(handler, forkableOptions...)

	numberedFileSourceFactory := bstream.SourceFactory(func(h bstream.Handler) bstream.Source {
		return f.fileSourceFactory(fileStartBlock, h)
	})

	if f.liveHeadTracker != nil {
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningLiveSourceWrapper(bstream.CloneBlock(forkHandler)))
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
	source, err := f.setupPipeline(ctx)
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
