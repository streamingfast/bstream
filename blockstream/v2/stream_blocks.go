package blockstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStopBlockReached = errors.New("stop block reached")

func (s Server) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	ctx := stream.Context()
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	var hasCursor bool
	var cursor *forkable.Cursor
	if request.StartCursor != "" {
		cur, err := forkable.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}
		cursor = cur
		hasCursor = true
	}

	stopNow := func(blockNum uint64) bool {
		return request.StopBlockNum > 0 && blockNum > request.StopBlockNum
	}

	var blockInterceptor func(blk interface{}) interface{}
	if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if stopNow(block.Number) {
			return errStopBlockReached
		}

		any, err := block.ToAny(true, blockInterceptor)
		if err != nil {
			return fmt.Errorf("to any: %w", err)
		}
		fObj := obj.(*forkable.ForkableObject)

		resp := &pbbstream.BlockResponseV2{
			Block:  any,
			Step:   forkable.StepToProto(fObj.Step),
			Cursor: fObj.Cursor().ToOpaque(),
		}
		if s.postHookFunc != nil {
			s.postHookFunc(ctx, resp)
		}
		err = stream.Send(resp)
		if err != nil {
			return err
		}

		return nil
	})

	var startBlock uint64
	var handler bstream.Handler
	var err error
	var fileStartBlock uint64

	joiningSourceOptions := []bstream.JoiningSourceOption{
		// First so JoiningSource can use it when dealing with other options
		bstream.JoiningSourceLogger(logger),
		// Overrides default behavior when targetBlockID is set, used as side effect of EternalSource
		bstream.JoiningSourceStartLiveImmediately(false),
	}
	forkableOptions := []forkable.Option{forkable.WithLogger(logger), forkable.WithFilters(forkable.StepsFromProto(request.ForkSteps))}

	if hasCursor {
		startBlock = cursor.StartBlockNum()
		forkableOptions = append(forkableOptions, forkable.FromCursor(cursor))
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(cursor.LIB.ID()))
		handler = handlerFunc

	} else {
		logger.Info("resolving relative block", zap.Int64("start_block", request.StartBlockNum))
		startBlock, err = s.tracker.GetRelativeBlock(ctx, request.StartBlockNum, bstream.BlockStreamHeadTarget)
		if err != nil {
			return fmt.Errorf("getting relative block: %w", err)
		}
		if request.StopBlockNum > 0 && startBlock > request.StopBlockNum {
			if request.StartBlockNum < 0 {
				return status.Errorf(codes.InvalidArgument, "resolved start block %d (relative %d) is after stop block %d", startBlock, request.StartBlockNum, request.StopBlockNum)
			}

			return status.Errorf(codes.InvalidArgument, "start block %d is after stop block %d", request.StartBlockNum, request.StopBlockNum)
		}

		logger.Info("resolving start block", zap.Uint64("start_block", startBlock))
		resolvedStartBlock, previousIrreversibleID, err := s.tracker.ResolveStartBlock(ctx, startBlock)
		if err != nil {
			return fmt.Errorf("failed to resolve start block: %w", err)
		}

		if previousIrreversibleID != "" {
			irrRef := bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
			logger.Debug("configuring inclusive LIB on forkable handler", zap.Stringer("irr_ref", irrRef))
			forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(irrRef))
			joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceTargetBlockID(previousIrreversibleID))
		}

		fileStartBlock = resolvedStartBlock
		handler = bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, handlerFunc, bstream.GateOptionWithLogger(logger))
	}

	// FIXME: How are we going to provide such stuff without depending on pbblockmeta ... would be a that bad since we already pull pbgo which contains it but still ...
	// if blockMeta != nil {
	// 	logger.Debug("configuring irreversibility checker on forkable handler")
	// 	forkableOptions = append(forkableOptions, forkable.WithIrreversibilityChecker(blockMeta, 1*time.Second))
	// }

	forkHandler := forkable.New(handler, forkableOptions...)

	var preproc bstream.PreprocessFunc
	if s.preprocFactory != nil {
		preproc, err = s.preprocFactory(request)
		if err != nil {
			return fmt.Errorf("filtering: %w", err)
		}
	}

	var liveSourceFactory bstream.SourceFactory
	if s.liveSourceFactory != nil {
		liveSourceFactory = bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
			if preproc != nil {
				// clone it before passing it to filtering processors, so it doesn't mutate
				// the subscriptionHub's Blocks and affects other subscribers to the hub.
				subHandler = bstream.CloneBlock(bstream.NewPreprocessor(preproc, subHandler))
			}

			return s.liveSourceFactory(subHandler)
		})
	}

	var fileSourceOptions []bstream.FileSourceOption
	if len(s.blocksStores) > 1 {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(s.blocksStores[1:]))
	}
	fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStores[0],
			fileStartBlock,
			1,
			preproc,
			subHandler,
			fileSourceOptions...,
		)
		return fs
	})

	if s.liveHeadTracker != nil {
		joiningSourceOptions = append(joiningSourceOptions, bstream.JoiningSourceLiveTracker(120, s.liveHeadTracker))
	}

	source := bstream.NewJoiningSource(fileSourceFactory, liveSourceFactory, forkHandler, joiningSourceOptions...)
	go func() {
		select {
		case <-ctx.Done():
			source.Shutdown(ctx.Err())
		}
	}()

	logger.Info("starting stream blocks",
		zap.String("req_cursor", request.StartCursor),
		zap.Int64("start_block", request.StartBlockNum),
		zap.Uint64("absolute_start_block", startBlock),
	)

	source.Run()
	if err := source.Err(); err != nil {
		if errors.Is(err, errStopBlockReached) {
			logger.Info("stream of blocks reached end block")
			return nil
		}

		// Most probably that the err came because the gRPC stream was teared down, nothing do to
		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, "source canceled")
		}

		// FIXME: Log to error if not some kind of transient error, how to determine it's a transient is the hard part
		logger.Info("unexpected stream of blocks termination", zap.Error(err))
		return status.Errorf(codes.Internal, "unexpected stream termination")
	}

	logger.Error("source is not expected to terminate gracefully, should stop at block or continue forever")
	return status.Error(codes.Internal, "unexpected stream completion")
}
