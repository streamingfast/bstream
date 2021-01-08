package blockstream

import (
	"fmt"
	"net"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/bstream/hub"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/opaque"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

type PreprocFactory func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)

type Server struct {
	blocksStores    []dstore.Store
	subscriptionHub *hub.SubscriptionHub
	grpcAddr        string
	tracker         *bstream.Tracker
	preprocFactory  func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)
	ready           bool
	trimmer         BlockTrimmer

	logger *zap.Logger
}

func NewServer(logger *zap.Logger, tracker *bstream.Tracker, blocksStores []dstore.Store, grpcAddr string, subscriptionHub *hub.SubscriptionHub, trimmer BlockTrimmer) *Server {
	t := tracker.Clone()
	t.AddGetter(bstream.BlockStreamHeadTarget, subscriptionHub.HeadTracker)
	return &Server{
		blocksStores:    blocksStores,
		grpcAddr:        grpcAddr,
		subscriptionHub: subscriptionHub,
		tracker:         t,
		trimmer:         trimmer,
		logger:          logger,
	}
}

func (s *Server) SetPreprocFactory(f PreprocFactory) {
	s.preprocFactory = f
}

func (s *Server) Serve() error {
	s.logger.Info("listening & serving blockstream gRPC service", zap.String("grpc_listen_addr", s.grpcAddr))
	grpcServer := dgrpc.NewServer(dgrpc.WithLogger(s.logger))
	pbbstream.RegisterBlockStreamV2Server(grpcServer, s)

	lis, err := net.Listen("tcp", s.grpcAddr)
	if err != nil {
		return fmt.Errorf("failed listening grpc %q: %w", s.grpcAddr, err)
	}

	if err := grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("error on gs.Serve: %w", err)
	}

	return nil
}

func (s *Server) SetReady() {
	s.ready = true
}

func (s *Server) IsReady() bool {
	return s.ready
}

func cursorToProto(rawCursor string) (out pbbstream.Cursor, err error) {
	payload, err := opaque.Decode(rawCursor)
	if err != nil {
		return out, fmt.Errorf("unable to decode: %w", err)
	}

	err = proto.Unmarshal(payload, &out)
	if err != nil {
		return out, fmt.Errorf("unable to unmarshal: %w", err)
	}

	return out, nil
}

func forkableStepToProto(step forkable.StepType) pbbstream.ForkStep {
	switch step {
	case forkable.StepNew:
		return pbbstream.ForkStep_STEP_NEW
	case forkable.StepUndo:
		return pbbstream.ForkStep_STEP_UNDO
	case forkable.StepIrreversible:
		return pbbstream.ForkStep_STEP_IRREVERSIBLE
	}
	return pbbstream.ForkStep_STEP_UNKNOWN
}

func forkableStepsFromProto(steps []pbbstream.ForkStep) forkable.StepType {
	if len(steps) <= 0 {
		return forkable.StepNew | forkable.StepUndo | forkable.StepIrreversible
	}

	var filter forkable.StepType
	for _, step := range steps {
		filter |= forkableStepFromProto(step)
	}
	return filter
}

func forkableStepFromProto(step pbbstream.ForkStep) forkable.StepType {
	switch step {
	case pbbstream.ForkStep_STEP_NEW:
		return forkable.StepNew
	case pbbstream.ForkStep_STEP_UNDO:
		return forkable.StepUndo
	case pbbstream.ForkStep_STEP_IRREVERSIBLE:
		return forkable.StepIrreversible
	}
	return forkable.StepType(0)
}
