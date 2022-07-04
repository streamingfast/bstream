package stream

import (
	"context"
	"errors"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

type Stream struct {
	liveSourceFactory bstream.SourceFactory
	blocksStore       dstore.Store

	startBlockNum                  int64
	stopBlockNum                   uint64
	irreversibleBlocksIndexStore   dstore.Store
	irreversibleBlocksIndexBundles []uint64

	handler            bstream.Handler
	preprocessFunc     bstream.PreprocessFunc
	preprocessThreads  int
	blockIndexProvider bstream.BlockIndexProvider

	finalBlocksOnly bool
	cursor          *bstream.Cursor
	tracker         *bstream.Tracker

	liveHeadTracker bstream.BlockRefGetter
	logger          *zap.Logger
}

func New(
	blocksStore dstore.Store,
	startBlockNum int64,
	handler bstream.Handler,
	options ...Option) *Stream {
	s := &Stream{
		blocksStore:   blocksStore,
		startBlockNum: startBlockNum,
		logger:        zap.NewNop(),
		handler:       handler,
	}

	for _, option := range options {
		option(s)
	}

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

	absoluteStartBlockNum, err := resolveNegativeStartBlockNum(ctx, s.startBlockNum, s.tracker)
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

	if s.finalBlocksOnly {
		if hasCursor {
			if !s.cursor.IsFinalOnly() {
				return nil, fmt.Errorf("invalid: cannot stream with final-blocks-only from this non-final cursor")
			}
			absoluteStartBlockNum = s.cursor.Block.Num()
		}
		sf := s.fileSourceFactory(absoluteStartBlockNum)
		return sf(s.wrappedHandler()), nil
	}

	if hasCursor && !s.cursor.IsFinalOnly() {
		panic("implement resolveCursor")
		//blocks, err := resolveCursor(ctx, s.cursor)
		// if err != nil {
		// 	return nil, fmt.Errorf("cannot resolve cursor: $w", err)
		// }
	}

	// FIXME:  we should have some joiningsource here, see if the hub can be used directly?
	sf := s.fileSourceFactory(absoluteStartBlockNum)
	return sf(s.wrappedHandler()), nil

	//	if hasCursor {
	//		cursorBlock = s.cursor.Block
	//		if s.cursor.Step != bstream.StepNew && s.cursor.Step != bstream.StepIrreversible {
	//			forkedCursor = true
	//		}
	//		irreversibleStartBlockNum = s.cursor.LIB.Num()
	//	}

	//	if !forkedCursor {
	//		if irrIndex := bstream.NewBlockIndexesManager(ctx, s.irreversibleBlocksIndexStore, s.irreversibleBlocksIndexBundles, irreversibleStartBlockNum, s.stopBlockNum, cursorBlock, s.blockIndexProvider); irrIndex != nil {
	//			return bstream.NewIndexedFileSource(
	//				s.wrappedHandler(),
	//				s.preprocessFunc,
	//				irrIndex,
	//				s.blocksStore,
	//				s.joiningSourceFactory(),
	//				s.forkableHandlerWrapper(nil, false, 0),
	//				s.logger,
	//				s.forkSteps,
	//				s.cursor,
	//			), nil
	//		}
	//	}
	//}

	// joiningSource -> forkable -> wrappedHandler

	//	return bstream.NewFileSource(s.blocksStore, absoluteStartBlockNum, h, s.logger), nil
	//	if hasCursor {
	//		forkableHandlerWrapper := s.forkableHandlerWrapper(s.cursor, true, absoluteStartBlockNum) // you don't want the cursor's block to be the lower limit
	//		forkableHandler := forkableHandlerWrapper(h, s.cursor.LIB)
	//		jsf := s.joiningSourceFactoryFromCursor(s.cursor)
	//
	//		return jsf(s.cursor.Block.Num(), forkableHandler), nil
	//	}

	// no cursor, no tracker, probably just block files on disk
	//forkableHandlerWrapper := s.forkableHandlerWrapper(nil, false, absoluteStartBlockNum)
	//forkableHandler := forkableHandlerWrapper(h, nil)
	//jsf := s.joiningSourceFactory()
	//	return jsf(absoluteStartBlockNum, forkableHandler), nil

}

func resolveNegativeStartBlockNum(ctx context.Context, startBlockNum int64, tracker *bstream.Tracker) (uint64, error) {
	if startBlockNum < 0 {
		absoluteValue, err := tracker.GetRelativeBlock(ctx, startBlockNum, bstream.BlockStreamHeadTarget)
		if err != nil {
			if errors.Is(err, bstream.ErrGetterUndefined) {
				return 0, NewErrInvalidArg("requested negative start block number (%d), but this instance has no HEAD tracker", startBlockNum)
			}
			return 0, fmt.Errorf("getting relative block: %w", err)
		}
		return absoluteValue, nil
	}
	return uint64(startBlockNum), nil
}

// adds stopBlock to the handler
func (s *Stream) wrappedHandler() bstream.Handler {

	h := s.handler

	if s.stopBlockNum > 0 {
		h = bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
			if block.Number > s.stopBlockNum {
				return ErrStopBlockReached
			}
			if err := s.handler.ProcessBlock(block, obj); err != nil {
				return err
			}

			if block.Number == s.stopBlockNum {
				return ErrStopBlockReached
			}
			return nil
		})
	}

	return h

}

func (s *Stream) forkableHandlerWrapper(cursor *bstream.Cursor, libInclusive bool, startBlockNum uint64) func(h bstream.Handler, lib bstream.BlockRef) bstream.Handler {
	return func(h bstream.Handler, lib bstream.BlockRef) bstream.Handler {

		forkableOptions := []forkable.Option{
			forkable.WithLogger(s.logger),
			//		forkable.WithFilters(s.forkSteps),
		}

		if !cursor.IsEmpty() {
			// does all the heavy lifting (setting the lib and start block, etc.)
			forkableOptions = append(forkableOptions, forkable.FromCursor(s.cursor))
		} else {
			if lib != nil {
				if libInclusive {
					s.logger.Debug("configuring inclusive LIB on forkable handler", zap.Stringer("lib", lib))
					forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(lib))
				} else {
					s.logger.Debug("configuring exclusive LIB on forkable handler", zap.Stringer("lib", lib))
					forkableOptions = append(forkableOptions, forkable.WithExclusiveLIB(lib))
				}
			}
		}

		return forkable.New(bstream.NewMinimalBlockNumFilter(startBlockNum, h), forkableOptions...)
	}
}

func (s *Stream) joiningSourceFactoryFromResolvedBlock(fileStartBlock uint64, previousIrreversibleID string) bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {

		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(s.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}

		if s.liveHeadTracker != nil {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, s.liveHeadTracker))
		}

		s.logger.Info("firehose pipeline bootstrapping from tracker",
			zap.Uint64("requested_start_block", startBlockNum),
			zap.Uint64("file_start_block", fileStartBlock),
			zap.String("previous_irr_id", previousIrreversibleID),
		)

		if previousIrreversibleID != "" {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(previousIrreversibleID))
		}

		return bstream.NewJoiningSource(s.fileSourceFactory(fileStartBlock), s.liveSourceFactory, h, joiningSourceOptions...)

	}
}

func (s *Stream) joiningSourceFactoryFromCursor(cursor *bstream.Cursor) bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {

		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(s.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}

		if s.liveHeadTracker != nil {
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, s.liveHeadTracker))
		}

		fileStartBlock := cursor.LIB.Num() // we don't use startBlockNum, the forkable will wait for the cursor before it forwards blocks
		if fileStartBlock < bstream.GetProtocolFirstStreamableBlock {
			s.logger.Info("adjusting requested file_start_block to protocol_first_streamable_block",
				zap.Uint64("file_start_block", fileStartBlock),
				zap.Uint64("protocol_first_streamable_block", bstream.GetProtocolFirstStreamableBlock),
			)
			fileStartBlock = bstream.GetProtocolFirstStreamableBlock
		}
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(cursor.LIB.ID()))

		s.logger.Info("firehose pipeline bootstrapping from cursor",
			zap.Uint64("file_start_block", fileStartBlock),
			zap.Stringer("cursor_lib", cursor.LIB),
		)
		return bstream.NewJoiningSource(s.fileSourceFactory(fileStartBlock), s.liveSourceFactory, h, joiningSourceOptions...)
	}
}

func (s *Stream) joiningSourceFactory() bstream.SourceFromNumFactory {
	return func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		joiningSourceOptions := []bstream.JoiningSourceOption{
			bstream.JoiningSourceLogger(s.logger),
			bstream.JoiningSourceStartLiveImmediately(false),
		}
		s.logger.Info("firehose pipeline bootstrapping",
			zap.Uint64("start_block", startBlockNum),
		)
		return bstream.NewJoiningSource(s.fileSourceFactory(startBlockNum), s.liveSourceFactory, h, joiningSourceOptions...)
	}
}

func (s *Stream) fileSourceFactory(startBlockNum uint64) bstream.SourceFactory {
	return func(h bstream.Handler) bstream.Source {
		var fileSourceOptions []bstream.FileSourceOption
		if s.preprocessFunc != nil {
			fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(s.preprocessFunc, s.preprocessThreads))
		}
		if s.stopBlockNum != 0 {
			fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithStopBlock(s.stopBlockNum))
		}

		fs := bstream.NewFileSource(
			s.blocksStore,
			startBlockNum,
			h,
			s.logger,
			fileSourceOptions...,
		)
		return fs
	}
}
