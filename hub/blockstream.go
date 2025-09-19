package hub

import (
	"context"
	"fmt"
	"time"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	ggrpcserver "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/logging"
	pbheadinfo "github.com/streamingfast/pbgo/sf/headinfo/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// implementation of blockstream.Server from the hub
func (h *ForkableHub) NewBlockstreamServer(dgrpcServer ggrpcserver.Server) *BlockstreamServer {

	bs := &BlockstreamServer{
		hub:         h,
		dgrpcServer: dgrpcServer,
	}

	pbheadinfo.RegisterHeadInfoServer(dgrpcServer.ServiceRegistrar(), bs)
	pbbstream.RegisterBlockStreamServer(dgrpcServer.ServiceRegistrar(), bs)
	return bs
}

type BlockstreamServer struct {
	hub         *ForkableHub
	dgrpcServer ggrpcserver.Server
}

func (s *BlockstreamServer) Launch(serverAddr string) {
	<-s.hub.Ready
	zlog.Info("blockstream server hub ready, launching", zap.String("server_addr", serverAddr))
	go s.dgrpcServer.Launch(serverAddr)
}

func (s *BlockstreamServer) Close() {
	s.dgrpcServer.Shutdown(0)
}

func (s *BlockstreamServer) GetHeadInfo(ctx context.Context, req *pbheadinfo.HeadInfoRequest) (*pbheadinfo.HeadInfoResponse, error) {
	num, id, t, libNum, err := s.hub.HeadInfo()
	if err != nil {
		return nil, err
	}

	resp := &pbheadinfo.HeadInfoResponse{
		LibNum:   libNum,
		HeadNum:  num,
		HeadID:   id,
		HeadTime: timestamppb.New(t),
	}
	return resp, nil
}

func (s *BlockstreamServer) Blocks(r *pbbstream.BlockRequest, stream pbbstream.BlockStream_BlocksServer) error {
	logger := logging.Logger(stream.Context(), zlog).Named("sub").Named(r.Requester)

	logger.Info("receive block request", zap.Reflect("request", r))

	h := streamHandler(stream, logger)
	var source bstream.Source

	if r.Burst == -1 {
		_, _, _, libNum, err := s.hub.HeadInfo()
		if err != nil {
			return err
		}
		source = s.hub.SourceFromBlockNumWithForks(libNum, h)
	} else if r.Burst < -1 {
		desiredBlock := uint64(-r.Burst)
		if lowestHub := s.hub.LowestBlockNum(); lowestHub > desiredBlock {
			desiredBlock = lowestHub
		}
		source = s.hub.SourceFromBlockNumWithForks(desiredBlock, h)
	} else {
		headNum, _, _, _, err := s.hub.HeadInfo()
		if err != nil {
			return err
		}
		var desiredBlock uint64
		if uint64(r.Burst) > headNum || headNum-uint64(r.Burst) < bstream.GetProtocolFirstStreamableBlock {
			desiredBlock = bstream.GetProtocolFirstStreamableBlock
		} else {
			desiredBlock = headNum - uint64(r.Burst)
		}

		if lowestHub := s.hub.LowestBlockNum(); lowestHub > desiredBlock {
			desiredBlock = lowestHub
		}
		source = s.hub.SourceFromBlockNumWithForks(desiredBlock, h)
	}

	if source == nil {
		return fmt.Errorf("cannot get source for request %+v", r)
	}
	source.Run()
	<-source.Terminated()
	if err := source.Err(); err != nil {
		return err
	}
	return nil
}

func (s *BlockstreamServer) BlocksAndSignals(r *pbbstream.BlocksAndSignalsRequest, stream pbbstream.BlockStream_BlocksAndSignalsServer) error {
	logger := logging.Logger(stream.Context(), zlog).Named("sub").Named(r.BlockRequest.Requester)

	logger.Info("receive blocks and signals request", zap.Reflect("request", r.BlockRequest))

	h := streamHandlerWithSignals(stream, logger)
	var source bstream.Source

	if r.BlockRequest.Burst == -1 {
		_, _, _, libNum, err := s.hub.HeadInfo()
		if err != nil {
			return err
		}
		source = s.hub.SourceFromBlockNumWithForks(libNum, h)
	} else if r.BlockRequest.Burst < -1 {
		desiredBlock := uint64(-r.BlockRequest.Burst)
		if lowestHub := s.hub.LowestBlockNum(); lowestHub > desiredBlock {
			desiredBlock = lowestHub
		}
		source = s.hub.SourceFromBlockNumWithForks(desiredBlock, h)
	} else {
		headNum, _, _, _, err := s.hub.HeadInfo()
		if err != nil {
			return err
		}
		var desiredBlock uint64
		if uint64(r.BlockRequest.Burst) > headNum || headNum-uint64(r.BlockRequest.Burst) < bstream.GetProtocolFirstStreamableBlock {
			desiredBlock = bstream.GetProtocolFirstStreamableBlock
		} else {
			desiredBlock = headNum - uint64(r.BlockRequest.Burst)
		}

		if lowestHub := s.hub.LowestBlockNum(); lowestHub > desiredBlock {
			desiredBlock = lowestHub
		}
		source = s.hub.SourceFromBlockNumWithForks(desiredBlock, h)
	}

	if source == nil {
		return fmt.Errorf("cannot get source for request %+v", r.BlockRequest)
	}
	source.Run()
	<-source.Terminated()
	if err := source.Err(); err != nil {
		return err
	}
	return nil
}

func streamHandler(stream pbbstream.BlockStream_BlocksServer, logger *zap.Logger) bstream.Handler {
	return bstream.NewHandler(
		func(blk *pbbstream.Block, _ any) error {
			err := stream.Send(blk)
			logger.Debug("block sent to stream", zap.Stringer("block", blk.AsRef()), zap.Duration("age", time.Since(blk.Timestamp.AsTime())), zap.Error(err))
			return err
		}, func(_ *pbbstream.Signal) error {
			return nil
		})
}

func streamHandlerWithSignals(stream pbbstream.BlockStream_BlocksAndSignalsServer, logger *zap.Logger) bstream.Handler {
	return bstream.NewHandler(
		func(blk *pbbstream.Block, _ interface{}) error {
			err := stream.Send(pbbstream.BlockToResponse(blk))
			logger.Debug("block sent to stream", zap.Stringer("block", blk.AsRef()), zap.Duration("age", time.Since(blk.Timestamp.AsTime())), zap.Error(err))
			return err
		},
		func(signal *pbbstream.Signal) error {
			err := stream.Send(pbbstream.SignalToResponse(signal))
			logger.Debug("signal sent to stream", zap.Stringer("signal", signal))
			return err
		},
	)
}
