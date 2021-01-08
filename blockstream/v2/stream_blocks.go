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
)

var errStopBlockReached = errors.New("stop block reached")

func (s Server) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	ctx := stream.Context()
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	startBlock, err := s.tracker.GetRelativeBlock(ctx, request.StartBlockNum, bstream.BlockStreamHeadTarget)
	if err != nil {
		return fmt.Errorf("getting relative block: %w", err)
	}

	stopNow := func(blockNum uint64) bool {
		return request.StopBlockNum > 0 && blockNum > request.StopBlockNum
	}

	logger.Info("resolving start block", zap.Uint64("start_block", startBlock))
	fileSourceStartBlockNum, previousIrreversibleBlockID, err := s.tracker.ResolveStartBlock(ctx, startBlock)
	if err != nil {
		return fmt.Errorf("failed to resolve start block: %w", err)
	}

	var preproc bstream.PreprocessFunc
	if s.preprocFactory != nil {
		preproc, err = s.preprocFactory(request)
		if err != nil {
			return fmt.Errorf("filtering: %w", err)
		}
	}

	var blockInterceptor func(blk interface{}) interface{}
	if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}

	var streamOut bstream.Handler
	handler := func(block *bstream.Block, obj interface{}) error {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("recovering from panic, temporary fix, please fix by removing bufferedHandler after hubsource", zap.Any("error", r))
			}
		}()

		if stopNow(block.Number) {
			return errStopBlockReached
		}

		any, err := block.ToAny(true, blockInterceptor)
		if err != nil {
			return fmt.Errorf("to any: %w", err)
		}

		err = stream.Send(&pbbstream.BlockResponseV2{
			Block:  any,
			Step:   toPBStep(obj.(*forkable.ForkableObject).Step),
			Cursor: blockToCursor(block),
		})
		if err != nil {
			return err
		}

		return nil
	}

	blocknumGate := bstream.NewBlockNumGate(startBlock, bstream.GateInclusive, bstream.HandlerFunc(handler))

	var filter forkable.StepType
	for _, step := range request.ForkSteps {
		fstep := fromPBStep(step)
		filter |= fstep
	}
	if filter == 0 {
		filter = forkable.StepsAll
	}
	streamOut = forkable.New(blocknumGate, forkable.WithFilters(filter))

	liveSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		if preproc != nil {
			// clone it before passing it to filtering processors, so it doesn't mutate
			// the subscriptionHub's Blocks and affects other subscribers to the hub.
			subHandler = bstream.CloneBlock(bstream.NewPreprocessor(preproc, subHandler))
		}
		return s.subscriptionHub.NewSource(subHandler /* burst */, 300)
	})

	var options []bstream.FileSourceOption
	if len(s.blocksStores) > 1 {
		options = append(options, bstream.FileSourceWithSecondaryBlocksStores(s.blocksStores[1:]))
	}
	fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStores[0],
			fileSourceStartBlockNum,
			1,
			preproc,
			subHandler,
			options...,
		)
		return fs
	})

	source := bstream.NewJoiningSource(
		fileSourceFactory,
		liveSourceFactory,
		streamOut,
		bstream.JoiningSourceLogger(logger),
		bstream.JoiningSourceTargetBlockID(previousIrreversibleBlockID),
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
		zap.Uint64("start_block", startBlock),
		zap.Uint64("file_start_block", fileSourceStartBlockNum),
		zap.String("previous_irreversible_block_id", previousIrreversibleBlockID),
	)
	source.Run()
	switch source.Err() {
	case nil:
		return nil // I doubt this can happen
	case errStopBlockReached:
		logger.Debug("stop block reached")
		return nil
	}
	return err
}

func blockToCursor(block *bstream.Block) (out string) {
	cursor := pbbstream.Cursor{BlockId: block.Id}
	payload, err := proto.Marshal(&cursor)
	if err != nil {
		panic(fmt.Errorf("unable to marshal cursor to binary: %w", err))
	}

	return opaque.Encode(payload)
}
