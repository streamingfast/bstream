package stream

import (
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

type Option = func(s *Stream)

func WithPreprocessFunc(pp bstream.PreprocessFunc) Option {
	return func(s *Stream) {
		s.preprocessFunc = pp
	}
}

func WithStreamBlocksParallelFiles(i int) Option {
	return func(s *Stream) {
		s.parallelFiles = i
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(s *Stream) {
		s.logger = logger
	}
}

func WithConfirmations(confirmations uint64) Option {
	return func(s *Stream) {
		s.confirmations = confirmations
	}
}

func WithForkableSteps(steps bstream.StepType) Option {
	return func(s *Stream) {
		s.forkSteps = steps
	}
}

func WithBlockIndexProvider(p bstream.BlockIndexProvider) Option {
	return func(s *Stream) {
		s.blockIndexProvider = p
	}
}

func WithCursor(cursor *bstream.Cursor) Option {
	return func(s *Stream) {
		s.cursor = cursor
	}
}

func WithLiveHeadTracker(liveHeadTracker bstream.BlockRefGetter) Option {
	return func(s *Stream) {
		s.liveHeadTracker = liveHeadTracker
	}
}

func WithTracker(tracker *bstream.Tracker) Option {
	return func(s *Stream) {
		s.tracker = tracker
	}
}

func WithStopBlock(stopBlockNum uint64) Option { //inclusive
	return func(s *Stream) {
		s.stopBlockNum = stopBlockNum
	}
}

func WithLiveSource(liveSourceFactory bstream.SourceFactory) Option {
	return func(s *Stream) {
		s.liveSourceFactory = func(h bstream.Handler) bstream.Source {
			return liveSourceFactory(h)
		}
	}
}
