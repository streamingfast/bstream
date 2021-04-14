package blockstream

import (
	"context"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
)

type PreprocFactory func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)

var StreamBlocksParallelFiles = 1

type Server struct {
	blocksStores      []dstore.Store
	liveSourceFactory bstream.SourceFactory
	liveHeadTracker   bstream.BlockRefGetter
	tracker           *bstream.Tracker
	preprocFactory    func(req *pbbstream.BlocksRequestV2) (bstream.PreprocessFunc, error)
	ready             bool
	trimmer           BlockTrimmer
	postHookFunc      func(context.Context, *pbbstream.BlockResponseV2)

	logger *zap.Logger
}

func NewServer(
	logger *zap.Logger,
	blocksStores []dstore.Store,
	liveSourceFactory bstream.SourceFactory,
	liveHeadTracker bstream.BlockRefGetter,
	tracker *bstream.Tracker,
	trimmer BlockTrimmer,
) *Server {
	t := tracker.Clone()
	if liveHeadTracker != nil {
		t.AddGetter(bstream.BlockStreamHeadTarget, liveHeadTracker)
	}

	return &Server{
		blocksStores:      blocksStores,
		liveSourceFactory: liveSourceFactory,
		liveHeadTracker:   liveHeadTracker,
		tracker:           t,
		trimmer:           trimmer,
		logger:            logger,
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
