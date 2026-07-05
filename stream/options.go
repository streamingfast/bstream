package stream

import (
	"github.com/streamingfast/bstream"
	"go.uber.org/zap"
)

const DefaultPreprocessFuncThreadNumber = 4

type Option = func(s *Stream)

func WithPreprocessFunc(pp bstream.PreprocessFunc, threads int) Option {
	return func(s *Stream) {
		s.preprocessFunc = pp
		s.preprocessThreads = threads
	}
}

func WithPreprocessFuncDefaultThreadNumber(pp bstream.PreprocessFunc) Option {
	return WithPreprocessFunc(pp, DefaultPreprocessFuncThreadNumber)
}

func WithLogger(logger *zap.Logger) Option {
	return func(s *Stream) {
		s.logger = logger
	}
}

func WithFinalBlocksOnly() Option {
	return func(s *Stream) {
		s.finalBlocksOnly = true
	}
}

func WithCustomStepTypeFilter(step bstream.StepType) Option {
	return func(s *Stream) {
		s.customStepTypeFilter = &step
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
func WithTargetCursor(cursor *bstream.Cursor) Option {
	return func(s *Stream) {
		s.cursor = cursor
		s.cursorIsTarget = true
	}
}

func WithStopBlock(stopBlockNum uint64) Option { //inclusive
	return func(s *Stream) {
		s.stopBlockNum = stopBlockNum
	}
}

// WithMergedBlocksBundleSize overrides the number of blocks per merged-blocks
// file for this stream. When not set, bstream.DefaultMergedBlocksBundleSize
// applies. The value must match the size of the files actually present in the
// merged-blocks store.
func WithMergedBlocksBundleSize(bundleSize uint64) Option {
	return func(s *Stream) {
		s.mergedBlocksBundleSize = bundleSize
	}
}

func WithLiveSourceHandlerMiddleware(mw func(source bstream.Handler) bstream.Handler) Option {
	return func(s *Stream) {
		s.liveSourceHandlerMiddleware = mw
	}
}

func WithFileSourceHandlerMiddleware(mw func(source bstream.Handler) bstream.Handler) Option {
	return func(s *Stream) {
		s.fileSourceHandlerMiddleware = mw
	}
}
