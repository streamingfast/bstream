package hub

import (
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	ggrpcserver "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/logging"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
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

func streamHandler(stream pbbstream.BlockStream_BlocksServer, logger *zap.Logger) bstream.Handler {
	return bstream.HandlerFunc(
		func(blk *bstream.Block, _ interface{}) error {
			block, err := blk.ToProto()
			if err != nil {
				panic(fmt.Errorf("unable to transform from bstream.Block to StreamableBlock: %w", err))
			}
			err = stream.Send(block)
			logger.Debug("block sent to stream", zap.Stringer("block", blk), zap.Error(err))
			return err
		})
}
