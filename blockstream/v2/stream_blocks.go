package blockstream

import (
	"errors"
	"fmt"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/logging"
	"github.com/dfuse-io/opaque"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStopBlockReached = errors.New("stop block reached")

func (s Server) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	ctx := stream.Context()
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	cursor, hasCursor, err := pbbstream.Cursor{}, false, (error)(nil)
	if request.StartCursor != "" {
		err := cursorToProto(request.StartCursor, &cursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		hasCursor = true
	}

	stopNow := func(blockNum uint64) bool {
		return request.StopBlockNum > 0 && blockNum > request.StopBlockNum
	}

	var blockInterceptor func(blk interface{}) interface{}
	if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}

	handler := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		if stopNow(block.Number) {
			return errStopBlockReached
		}

		any, err := block.ToAny(true, blockInterceptor)
		if err != nil {
			return fmt.Errorf("to any: %w", err)
		}

		resp := &pbbstream.BlockResponseV2{
			Block:  any,
			Step:   forkableStepToProto(obj.(*forkable.ForkableObject).Step),
			Cursor: blockToCursor(block),
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

	startBlock := cursor.BlockNum
	if !hasCursor {
		logger.Info("resolving relative block", zap.Int64("start_block", request.StartBlockNum))
		startBlock, err = s.tracker.GetRelativeBlock(ctx, request.StartBlockNum, bstream.BlockStreamHeadTarget)
		if err != nil {
			return fmt.Errorf("getting relative block: %w", err)
		}
	}

	if !hasCursor && request.StopBlockNum > 0 && startBlock > request.StopBlockNum {
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

	var startBlockGate bstream.Handler
	if hasCursor {
		startBlockGate = bstream.NewBlockIDGate(cursor.BlockId, bstream.GateExclusive, handler, bstream.GateOptionWithLogger(logger))
	} else {
		startBlockGate = bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, handler, bstream.GateOptionWithLogger(logger))
	}

	forkableOptions := []forkable.Option{forkable.WithLogger(logger), forkable.WithFilters(forkableStepsFromProto(request.ForkSteps))}
	if previousIrreversibleID != "" {
		irrRef := bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
		logger.Debug("configuring inclusive LIB on forkable handler", zap.Stringer("irr_ref", irrRef))
		forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(irrRef))
	}

	// FIXME: How are we going to provide such stuff without depending on pbblockmeta ... would be a that bad since we already pull pbgo which contains it but still ...
	// if blockMeta != nil {
	// 	logger.Debug("configuring irreversibility checker on forkable handler")
	// 	forkableOptions = append(forkableOptions, forkable.WithIrreversibilityChecker(blockMeta, 1*time.Second))
	// }

	forkHandler := forkable.New(startBlockGate, forkableOptions...)

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
			resolvedStartBlock,
			1,
			preproc,
			subHandler,
			fileSourceOptions...,
		)
		return fs
	})

	joinAtBlockID := cursor.BlockId
	if !hasCursor {
		joinAtBlockID = previousIrreversibleID
	}

	options := []bstream.JoiningSourceOption{
		// First so JoiningSource can use it when dealing with other options
		bstream.JoiningSourceLogger(logger),
		// Overrides default behavior when targetBlockID is set, used as side effect of EternalSource
		bstream.JoiningSourceStartLiveImmediately(false),
		bstream.JoiningSourceTargetBlockID(joinAtBlockID),
	}

	if s.liveHeadTracker != nil {
		options = append(options, bstream.JoiningSourceLiveTracker(120, s.liveHeadTracker))
	}

	source := bstream.NewJoiningSource(fileSourceFactory, liveSourceFactory, forkHandler, options...)
	go func() {
		select {
		case <-ctx.Done():
			source.Shutdown(ctx.Err())
		}
	}()

	logger.Info("starting stream blocks",
		zap.Bool("from_cursor", hasCursor),
		zap.Int64("start_block", request.StartBlockNum),
		zap.Uint64("absolute_start_block", startBlock),
		zap.Uint64("resolved_start_block", resolvedStartBlock),
		zap.String("previous_irreversible_block_id", previousIrreversibleID),
		zap.String("join_at_block_id", joinAtBlockID),
	)

	source.Run()
	if err := source.Err(); err != nil {
		if errors.Is(err, errStopBlockReached) {
			logger.Info("stream of blocks reached end block")
			return nil
		}

		// FIXME: Log to error if not some kind of transient error, how to determine it's a transient is the hard part, also, obfuscate
		logger.Info("unexpected stream of blocks termination", zap.Error(err))
		return status.Errorf(codes.Internal, "unexpected stream termination")
	}

	logger.Error("source is not expected to terminate gracefully, should stop at block or continue forever")
	return status.Error(codes.Internal, "unexpected stream completion")
}

func blockToCursor(block *bstream.Block) (out string) {
	cursor := pbbstream.Cursor{BlockId: block.Id, BlockNum: block.Number}
	payload, err := proto.Marshal(&cursor)
	if err != nil {
		panic(fmt.Errorf("unable to marshal cursor to binary: %w", err))
	}

	return opaque.Encode(payload)
}
