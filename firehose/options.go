package firehose

import (
	"sort"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type Option = func(s *Firehose)

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

		sort.Slice(f.irreversibleBlocksIndexBundles, func(i, j int) bool { return f.irreversibleBlocksIndexBundles[i] > f.irreversibleBlocksIndexBundles[j] })
	}
}

func WithLiveSource(liveSourceFactory bstream.SourceFactory) Option {
	return func(f *Firehose) {
		f.liveSourceFactory = func(h bstream.Handler) bstream.Source {
			return liveSourceFactory(h)
		}
	}
}
