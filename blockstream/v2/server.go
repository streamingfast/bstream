package blockstream

import (
	"context"
	"fmt"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/bstream/hub"
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
	tracker         *bstream.Tracker
	preprocFactory  func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)
	ready           bool
	trimmer         BlockTrimmer
	postHookFunc    func(context.Context, *pbbstream.BlockResponseV2)

	logger *zap.Logger
}

func NewServer(logger *zap.Logger, tracker *bstream.Tracker, blocksStores []dstore.Store, subscriptionHub *hub.SubscriptionHub, trimmer BlockTrimmer) *Server {
	t := tracker.Clone()
	t.AddGetter(bstream.BlockStreamHeadTarget, subscriptionHub.HeadTracker)
	return &Server{
		blocksStores:    blocksStores,
		subscriptionHub: subscriptionHub,
		tracker:         t,
		trimmer:         trimmer,
		logger:          logger,
	}
}

func (s *Server) SetPreprocFactory(f PreprocFactory) {
	s.preprocFactory = f
}

func (s *Server) SetPostHook(f func(ctx context.Context, response *pbbstream.BlockResponseV2)) {
	s.postHookFunc = f
}

func (s *Server) SetReady() {
	s.ready = true
}

func (s *Server) IsReady() bool {
	return s.ready
}

func cursorToProto(rawCursor string, into *pbbstream.Cursor) (err error) {
	payload, err := opaque.Decode(rawCursor)
	if err != nil {
		return fmt.Errorf("unable to decode: %w", err)
	}

	err = proto.Unmarshal(payload, into)
	if err != nil {
		return fmt.Errorf("unable to unmarshal: %w", err)
	}

	return nil
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
