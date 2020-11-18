package blockstream

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/bstream/hub"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

type PreprocFactory func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)

type Server struct {
	blocksStore     dstore.Store
	subscriptionHub *hub.SubscriptionHub
	grpcAddr        string
	tracker         *bstream.Tracker
	preprocFactory  func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)
	ready           bool
}

func NewServer(tracker *bstream.Tracker, blocksStore dstore.Store, grpcAddr string, subscriptionHub *hub.SubscriptionHub) *Server {
	t := tracker.Clone()
	t.AddGetter(bstream.BlockStreamHeadTarget, subscriptionHub.HeadTracker)
	return &Server{
		blocksStore:     blocksStore,
		grpcAddr:        grpcAddr,
		subscriptionHub: subscriptionHub,
		tracker:         t,
	}

	//
}

func (s *Server) SetPreprocFactory(f PreprocFactory) {
	s.preprocFactory = f
}

func (s Server) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	zlog.Info("incoming bstreamv2 Blocks request", zap.Reflect("req", request))

	ctx := stream.Context()

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
					zlog.Error("recovering from panic, temporary fix, please fix by removing bufferedHandler after hubsource")
				}
			}()

			if stopNow(block.Number) {
				return fmt.Errorf("reached stop block")
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
		handle := func(block *bstream.Block, obj interface{}) error {
			defer func() {
				if r := recover(); r != nil {
					zlog.Error("recovering from panic, temporary fix, please fix by removing bufferedHandler after hubsource")
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

		streamOut = bstream.NewBlockNumGate(startBlock, gateType, bstream.HandlerFunc(handle))
	}

	fileSourceStartBlockNum, fileSourceStartBlockID, err := s.tracker.ResolveStartBlock(ctx, startBlock)
	if err != nil {
		err = fmt.Errorf("failed to resolve start block: %w", err)
	}

	zlog.Info("starting stream blocks", zap.Uint64("start_block", startBlock))

	liveSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		if preproc != nil {
			// clone it before passing it to filtering processors, so it doesn't mutate
			// the subscriptionHub's Blocks and affects other subscribers to the hub.
			subHandler = bstream.CloneBlock(bstream.NewPreprocessor(preproc, subHandler))
		}
		return s.subscriptionHub.NewSource(subHandler /* burst */, 300)
	})

	fileSourceFactory := bstream.SourceFactory(func(subHandler bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStore,
			fileSourceStartBlockNum,
			1,
			preproc,
			subHandler,
		)
		return fs
	})

	source := bstream.NewJoiningSource(
		fileSourceFactory,
		liveSourceFactory,
		streamOut,
		bstream.JoiningSourceLogger(zlog),
		bstream.JoiningSourceTargetBlockID(fileSourceStartBlockID),
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
	err = source.Err()
	// FIXME: Really hackish way to check if context canceled, actual `err` type here is
	//        `*status.statusError`, so make our from there instead.
	if err != nil && !strings.HasSuffix(err.Error(), context.Canceled.Error()) {
		zlog.Error("stream blocks live source abnormal termination", zap.Error(err))
		return err
	}

	zlog.Debug("stream blocks terminated")
	return nil
}

func (s *Server) Serve() error {
	zlog.Info("listening & serving blockstream gRPC service", zap.String("grpc_listen_addr", s.grpcAddr))
	grpcServer := dgrpc.NewServer(dgrpc.WithLogger(zlog))
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
