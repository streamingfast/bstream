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

	cursor, hasCursor := (*pbbstream.Cursor)(nil), false
	if request.StartCursor != "" {
		decoderCursor, err := cursorToProto(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		cursor = &decoderCursor
		_, _ = cursor, hasCursor
	}

	stopNow := func(blockNum uint64) bool {
		return request.StopBlockNum > 0 && blockNum > request.StopBlockNum
	}

	var blockInterceptor func(blk interface{}) interface{}
	if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}

	handler := func(block *bstream.Block, obj interface{}) error {
		if stopNow(block.Number) {
			return errStopBlockReached
		}

		any, err := block.ToAny(true, blockInterceptor)
		if err != nil {
			return fmt.Errorf("to any: %w", err)
		}

		err = stream.Send(&pbbstream.BlockResponseV2{
			Block:  any,
			Step:   forkableStepToProto(obj.(*forkable.ForkableObject).Step),
			Cursor: blockToCursor(block),
		})
		if err != nil {
			return err
		}

		return nil
	}

	logger.Info("resolving relative block", zap.Int64("start_block", request.StartBlockNum))
	startBlock, err := s.tracker.GetRelativeBlock(ctx, request.StartBlockNum, bstream.BlockStreamHeadTarget)
	if err != nil {
		return fmt.Errorf("getting relative block: %w", err)
	}

	logger.Info("resolving start block", zap.Uint64("start_block", startBlock))
	resolvedStartBlock, previousIrreversibleID, err := s.tracker.ResolveStartBlock(ctx, startBlock)
	if err != nil {
		return fmt.Errorf("failed to resolve start block: %w", err)
	}

	startBlockGate := bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, bstream.HandlerFunc(handler), bstream.GateOptionWithLogger(logger))

	forkableOptions := []forkable.Option{forkable.WithLogger(logger), forkable.WithFilters(forkableStepsFromProto(request.ForkSteps))}
	if previousIrreversibleID != "" {
		irrRef := bstream.NewBlockRef(previousIrreversibleID, resolvedStartBlock)
		logger.Debug("configuring inclusive LIB on forkable handler", zap.Stringer("irr_ref", irrRef))
		forkableOptions = append(forkableOptions, forkable.WithInclusiveLIB(irrRef))
	}

	// Configuration from cursor ...
	// if !bstream.EqualsBlockRefs(startBlock, bstream.BlockRefEmpty) {
	// 	// Only when we do **not** start from the beginning (i.e. startBlock is the empty block ref), that the
	// 	// forkable should be initialized with an initial LIB value. Otherwise, when we start fresh, the forkable
	// 	// will automatically set its LIB to the first streamable block of the chain.
	// 	forkableOptions = append(forkableOptions, forkable.WithExclusiveLIB(startBlock))
	// }

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

	liveSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		if preproc != nil {
			// clone it before passing it to filtering processors, so it doesn't mutate
			// the subscriptionHub's Blocks and affects other subscribers to the hub.
			subHandler = bstream.CloneBlock(bstream.NewPreprocessor(preproc, subHandler))
		}
		return s.subscriptionHub.NewSource(subHandler, 250)
	})

	var options []bstream.FileSourceOption
	if len(s.blocksStores) > 1 {
		options = append(options, bstream.FileSourceWithSecondaryBlocksStores(s.blocksStores[1:]))
	}
	fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStores[0],
			resolvedStartBlock,
			1,
			preproc,
			subHandler,
			options...,
		)
		return fs
	})

	// When using back cursor
	// bstream.JoiningSourceTargetBlockID(startBlock.ID()),
	// bstream.JoiningSourceTargetBlockNum(bstream.GetProtocolFirstStreamableBlock),

	source := bstream.NewJoiningSource(
		fileSourceFactory,
		liveSourceFactory,
		forkHandler,
		bstream.JoiningSourceLogger(logger),
		bstream.JoiningSourceLiveTracker(120, s.subscriptionHub.HeadTracker),
		// Overrides default behavior when targetBlockID is set, used as side effect of EternalSource
		bstream.JoiningSourceStartLiveImmediately(false),
	)

	go func() {
		select {
		case <-ctx.Done():
			source.Shutdown(ctx.Err())
		}
	}()

	logger.Info("starting stream blocks",
		zap.Int64("start_block", request.StartBlockNum),
		zap.Uint64("absolute_start_block", startBlock),
		zap.Uint64("resolved_start_block", resolvedStartBlock),
		zap.String("previous_irreversible_block_id", previousIrreversibleID),
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
	cursor := pbbstream.Cursor{BlockId: block.Id}
	payload, err := proto.Marshal(&cursor)
	if err != nil {
		panic(fmt.Errorf("unable to marshal cursor to binary: %w", err))
	}

	return opaque.Encode(payload)
}
