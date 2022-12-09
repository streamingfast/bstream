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
