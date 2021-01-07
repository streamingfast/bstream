package blockstream

import (
	"errors"
	"fmt"
	"net"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/bstream/hub"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

var errStopBlockReached = errors.New("stop block reached")

type PreprocFactory func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)

type Server struct {
	blocksStores    []dstore.Store
	subscriptionHub *hub.SubscriptionHub
	grpcAddr        string
	tracker         *bstream.Tracker
	preprocFactory  func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)
	ready           bool

	logger *zap.Logger
}

func NewServer(logger *zap.Logger, tracker *bstream.Tracker, blocksStores []dstore.Store, grpcAddr string, subscriptionHub *hub.SubscriptionHub) *Server {
	t := tracker.Clone()
	t.AddGetter(bstream.BlockStreamHeadTarget, subscriptionHub.HeadTracker)
	return &Server{
		blocksStores:    blocksStores,
		grpcAddr:        grpcAddr,
		subscriptionHub: subscriptionHub,
		tracker:         t,
		logger:          logger,
	}
}

func (s *Server) SetPreprocFactory(f PreprocFactory) {
	s.preprocFactory = f
}

func (s Server) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	ctx := stream.Context()
	logger := logging.Logger(ctx, s.logger)

	logger.Info("incoming blocks request", zap.Reflect("req", request))
	startBlock, err := s.tracker.GetRelativeBlock(ctx, request.StartBlockNum, bstream.BlockStreamHeadTarget)
	if err != nil {
		return fmt.Errorf("getting relative block: %w", err)
	}

	gateType := bstream.GateInclusive
	if request.ExcludeStartBlock {
		gateType = bstream.GateExclusive
	}

	var stopNow func(uint64) bool
	if request.StopBlockNum != 0 {
		stopNow = func(blockNum uint64) bool {
			if blockNum >= request.StopBlockNum {
				if !request.ExcludeStopBlock && blockNum == request.StopBlockNum {
					return false // let this one through
				}
				return true
			}
			return false
		}
	} else {
		stopNow = func(_ uint64) bool {
			return false
		}
	}

	var preproc bstream.PreprocessFunc
	if s.preprocFactory != nil {
		preproc, err = s.preprocFactory(request)
		if err != nil {
			return fmt.Errorf("filtering: %w", err)
		}
	}

	var streamOut bstream.Handler

	if request.HandleForks {
		handler := func(block *bstream.Block, obj interface{}) error {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("recovering from panic, temporary fix, please fix by removing bufferedHandler after hubsource")
				}
			}()

			if stopNow(block.Number) {
				return errStopBlockReached
			}

			fObj := obj.(*forkable.ForkableObject)
			any, err := block.ToAny(request.Decoded)
			if err != nil {
				return fmt.Errorf("to any: %w", err)
			}

			err = stream.Send(&pbbstream.BlockResponseV2{
				Undo:      fObj.Step == forkable.StepUndo,
				Step:      toPBStep(fObj.Step),
				StepCount: uint64(fObj.StepCount),
				StepIndex: uint64(fObj.StepIndex),
				Block:     any,
			})
			if err != nil {
				return err
			}

			return nil
		}

		blocknumGate := bstream.NewBlockNumGate(startBlock, gateType, bstream.HandlerFunc(handler))

		var filter forkable.StepType
		for _, step := range request.HandleForksSteps {
			fstep := fromPBStep(step)
			filter |= fstep
		}
		if filter == 0 {
			filter = forkable.StepsAll
		}
		streamOut = forkable.New(blocknumGate, forkable.WithFilters(filter))

	} else {
		handler := func(block *bstream.Block, _ interface{}) error {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("recovering from panic, temporary fix, please fix by removing bufferedHandler after hubsource")
				}
			}()

			if stopNow(block.Number) {
				return fmt.Errorf("reached stop block")
			}

			any, err := block.ToAny(request.Decoded)
			if err != nil {
				return fmt.Errorf("to any: %w", err)
			}

			return stream.Send(&pbbstream.BlockResponseV2{
				Block: any,
			})
		}

		streamOut = bstream.NewBlockNumGate(startBlock, gateType, bstream.HandlerFunc(handler))
	}

	fileSourceStartBlockNum, previousIrreversibleBlockID, err := s.tracker.ResolveStartBlock(ctx, startBlock)
	if err != nil {
		err = fmt.Errorf("failed to resolve start block: %w", err)
	}

	logger.Info("starting stream blocks",
		zap.Uint64("start_block", startBlock),
		zap.Uint64("file_start_block", fileSourceStartBlockNum),
		zap.String("previous_irreversible_block_id", previousIrreversibleBlockID),
	)

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
		bstream.JoiningSourceTargetBlockNum(fileSourceStartBlockNum),
		bstream.JoiningSourceLiveTracker(120 /* blocks considered near */, s.subscriptionHub.HeadTracker),
	)

	go func() {
		select {
		case <-ctx.Done():
			source.Shutdown(ctx.Err())
		}
	}()

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

func (s *Server) Serve() error {
	s.logger.Info("listening & serving blockstream gRPC service", zap.String("grpc_listen_addr", s.grpcAddr))
	grpcServer := dgrpc.NewServer(dgrpc.WithLogger(s.logger))
	pbbstream.RegisterBlockStreamV2Server(grpcServer, s)
	//pbhealth.RegisterHealthServer(grpcServer, s)

	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("failed listening grpc %q: %w", s.grpcAddr, err)
	}

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("error on gs.Serve: %w", err)
	}

	return nil
}

func toPBStep(step forkable.StepType) pbbstream.ForkStep {
	switch step {
	case forkable.StepNew:
		return pbbstream.ForkStep_STEP_NEW
	case forkable.StepUndo:
		return pbbstream.ForkStep_STEP_UNDO
	case forkable.StepRedo:
		return pbbstream.ForkStep_STEP_REDO
	case forkable.StepIrreversible:
		return pbbstream.ForkStep_STEP_IRREVERSIBLE
	case forkable.StepStalled:
		return pbbstream.ForkStep_STEP_STALLED
	}
	return pbbstream.ForkStep_STEP_UNKNOWN
}

func fromPBStep(step pbbstream.ForkStep) forkable.StepType {
	switch step {
	case pbbstream.ForkStep_STEP_NEW:
		return forkable.StepNew
	case pbbstream.ForkStep_STEP_UNDO:
		return forkable.StepUndo
	case pbbstream.ForkStep_STEP_REDO:
		return forkable.StepRedo
	case pbbstream.ForkStep_STEP_IRREVERSIBLE:
		return forkable.StepIrreversible
	case pbbstream.ForkStep_STEP_STALLED:
		return forkable.StepStalled
	}
	return forkable.StepType(0)
}

func (s *Server) SetReady() {
	s.ready = true
}

func (s *Server) IsReady() bool {
	return s.ready
}
