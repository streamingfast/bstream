package stream

import (
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/hub"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type Stream struct {
	fileSourceFactory bstream.ForkableSourceFactory
	liveSourceFactory bstream.ForkableSourceFactory

	currentHeadGetter func() uint64

	startBlockNum int64
	handler       bstream.Handler

	cursor       *bstream.Cursor
	stopBlockNum uint64

	preprocessFunc    bstream.PreprocessFunc
	preprocessThreads int

	blockIndexProvider bstream.BlockIndexProvider

	finalBlocksOnly      bool
	customStepTypeFilter *bstream.StepType

	logger *zap.Logger
}

func New(
	forkedBlocksStore dstore.Store,
	mergedBlocksStore dstore.Store,
	hub *hub.ForkableHub,
	startBlockNum int64,
	handler bstream.Handler,
	options ...Option) *Stream {

	s := &Stream{
		liveSourceFactory: hub,
		currentHeadGetter: hub.HeadNum,
		startBlockNum:     startBlockNum,
		handler:           handler,
		logger:            zap.NewNop(),
	}

	for _, option := range options {
		option(s)
	}

	var fileSourceOptions []bstream.FileSourceOption
	if s.stopBlockNum != 0 {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithStopBlock(s.stopBlockNum)) // more efficient than using our own
	}
	if s.preprocessFunc != nil {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(s.preprocessFunc, s.preprocessThreads))
	}
	if s.blockIndexProvider != nil {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithBlockIndexProvider(s.blockIndexProvider))
	}

	s.fileSourceFactory = bstream.NewFileSourceFactory(
		mergedBlocksStore,
		forkedBlocksStore,
		s.logger,
		fileSourceOptions...,
	)

	return s
}

func (s *Stream) Run(ctx context.Context) error {
	source, err := s.createSource(ctx)
	if err != nil {
		return err
	}

	go func() {
		select {
		case <-source.Terminated():
			return
		case <-ctx.Done():
			source.Shutdown(ctx.Err())
		}
	}()

	source.Run()
	if err := source.Err(); err != nil {
		s.logger.Debug("source shutting down", zap.Error(err))
		return err
	}
	return nil
}

func (s *Stream) createSource(ctx context.Context) (bstream.Source, error) {
	s.logger.Debug("setting up firehose source")

	absoluteStartBlockNum, err := resolveNegativeStartBlockNum(s.startBlockNum, s.currentHeadGetter)
	if err != nil {
		return nil, err
	}
	if absoluteStartBlockNum < bstream.GetProtocolFirstStreamableBlock {
		absoluteStartBlockNum = bstream.GetProtocolFirstStreamableBlock
	}
	if s.stopBlockNum > 0 && absoluteStartBlockNum > s.stopBlockNum {
		return nil, NewErrInvalidArg("start block %d is after stop block %d", absoluteStartBlockNum, s.stopBlockNum)
	}

	hasCursor := !s.cursor.IsEmpty()

	h := s.handler
	if s.stopBlockNum != 0 {
		h = stopBlockHandler(s.stopBlockNum, h)
	}

	if s.finalBlocksOnly {
		h = finalBlocksFilterHandler(h)
	} else if s.customStepTypeFilter != nil {
		h = customStepFilterHandler(*s.customStepTypeFilter, h)
	} else {
		h = newOrUndoFilterHandler(h)
	}

	if s.preprocessFunc != nil {
		h = bstream.NewPreprocessor(s.preprocessFunc, h)
	}

	if s.finalBlocksOnly && hasCursor && !s.cursor.IsOnFinalBlock() {
		return nil, NewErrInvalidArg("cannot stream with final-blocks-only from this non-final cursor")
	}

	return bstream.NewJoiningSource(
		s.fileSourceFactory,
		s.liveSourceFactory,
		h,
		ctx,
		absoluteStartBlockNum,
		s.cursor,
		s.logger,
	), nil

}

func resolveNegativeStartBlockNum(startBlockNum int64, currentHeadGetter func() uint64) (uint64, error) {
	if startBlockNum < 0 {
		if currentHeadGetter == nil {
			return 0, fmt.Errorf("cannot resolve negative start block: no headGetter set up")
		}
		delta := uint64(-startBlockNum)
		head := currentHeadGetter()
		if head < delta {
			return 0, nil
		}
		return uint64(head - delta), nil
	}
	return uint64(startBlockNum), nil
}

// StepNew, StepNewIrreversible and StepUndo will go through
func newOrUndoFilterHandler(h bstream.Handler) bstream.Handler {
	return bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if obj.(bstream.Stepable).Step().Matches(bstream.StepNew) || obj.(bstream.Stepable).Step().Matches(bstream.StepUndo) {
			return h.ProcessBlock(block, obj)
		}
		return nil
	})
}

// StepIrreversible and StepNewIrreversible will go through
func finalBlocksFilterHandler(h bstream.Handler) bstream.Handler {
	return bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if obj.(bstream.Stepable).Step().Matches(bstream.StepIrreversible) {
			return h.ProcessBlock(block, obj)
		}
		return nil
	})
}

func customStepFilterHandler(step bstream.StepType, h bstream.Handler) bstream.Handler {
	return bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if obj.(bstream.Stepable).Step().Matches(step) {
			return h.ProcessBlock(block, obj)
		}
		return nil
	})
}

func stopBlockHandler(stopBlockNum uint64, h bstream.Handler) bstream.Handler {
	if stopBlockNum > 0 {
		return bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
			if block.Number > stopBlockNum {
				return ErrStopBlockReached
			}
			if err := h.ProcessBlock(block, obj); err != nil {
				return err
			}

			if block.Number == stopBlockNum {
				return ErrStopBlockReached
			}
			return nil
		})
	}
	return h
}
