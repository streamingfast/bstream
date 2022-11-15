// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockstream

import (
	"context"
	"fmt"
	"sync"
	"time"

	dgrpcserver "github.com/streamingfast/dgrpc/server"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	pbheadinfo "github.com/streamingfast/pbgo/sf/headinfo/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServerOption func(s *Server)

func ServerOptionWithLogger(logger *zap.Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

func ServerOptionWithBuffer(size int) ServerOption {
	return func(s *Server) {
		s.buffer = bstream.NewBuffer("blockserver", s.logger.Named("buffer"))
		s.bufferSize = size
	}
}

type Server struct {
	headInfo      *headInfo
	buffer        *bstream.Buffer
	bufferSize    int
	subscriptions []*subscription
	grpcServer    dgrpcserver.Server

	lock sync.RWMutex

	logger *zap.Logger
}

func NewBufferedServer(server dgrpcserver.Server, size int, opts ...ServerOption) *Server {
	bs := NewServer(server, opts...)
	bs.buffer = bstream.NewBuffer("blockserver", bs.logger.Named("buffer"))
	bs.bufferSize = size
	return bs
}

func NewServer(server dgrpcserver.Server, opts ...ServerOption) *Server {
	s := NewUnmanagedServer(opts...)
	s.grpcServer = server

	pbheadinfo.RegisterHeadInfoServer(s.grpcServer.ServiceRegistrar(), s)
	pbbstream.RegisterBlockStreamServer(s.grpcServer.ServiceRegistrar(), s)
	return s
}

// NewUnmanagedServer can be used to create a BlockStream server that has no control
// over the actual gRPC server. Registration of services should be made manually by
// entity calling `blockstream.NewUnmanagedServer`.
//
// Moreover, management methods like `Close`
func NewUnmanagedServer(opts ...ServerOption) *Server {
	s := &Server{
		logger: zlog,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

type headInfo struct {
	libNum uint64
	Num    uint64
	ID     string
	time   *timestamp.Timestamp
}

func (s *Server) GetHeadInfo(ctx context.Context, req *pbheadinfo.HeadInfoRequest) (*pbheadinfo.HeadInfoResponse, error) {
	if s.headInfo == nil {
		return nil, status.Errorf(codes.Unavailable, "not ready")
	}

	hi := s.headInfo

	resp := &pbheadinfo.HeadInfoResponse{
		LibNum:   hi.libNum,
		HeadNum:  hi.Num,
		HeadID:   hi.ID,
		HeadTime: hi.time,
	}
	return resp, nil
}

func (s *Server) Blocks(r *pbbstream.BlockRequest, stream pbbstream.BlockStream_BlocksServer) error {
	logger := logging.Logger(stream.Context(), s.logger).Named("sub").Named(r.Requester)

	logger.Info("receive block request", zap.Reflect("request", r))
	subscription := s.subscribe(int(r.Burst), r.Requester)
	if subscription == nil {
		return fmt.Errorf("failed to create subscription for subscriber %q", r.Requester)
	}
	defer s.unsubscribe(subscription)

	for {
		select {
		// FIXME (MATT): We need to handle the case where the subscription directly closed the incoming block channel
		case <-stream.Context().Done():
			return nil
		case blk, ok := <-subscription.incomingBlock:
			if !ok {
				// we've been shutdown somehow, simply close the current connection..
				// we'll have logged at the source
				return nil
			}

			logger.Debug("sending block to subscription", zap.Stringer("block", blk))
			block, err := blk.ToProto()
			if err != nil {
				panic(fmt.Errorf("unable to transform from bstream.Block to StreamableBlock: %w", err))
			}

			err = stream.Send(block)
			logger.Debug("block sent to stream", zap.Stringer("block", blk))
			if err != nil {
				logger.Info("failed writing to socket, shutting down subscription", zap.Error(err))
				return nil
			}
		}
	}
}

func (s *Server) Ready() bool {
	return s.buffer == nil || (s.buffer.Len() >= s.bufferSize)
}

func (s *Server) SetHeadInfo(num uint64, id string, blkTime time.Time, libNum uint64) {
	t, err := ptypes.TimestampProto(blkTime)
	if err != nil {
		t = nil
	}
	s.headInfo = &headInfo{
		libNum: libNum,
		Num:    num,
		ID:     id,
		time:   t,
	}
}

func (s *Server) PushBlock(blk *bstream.Block) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	s.SetHeadInfo(blk.Num(), blk.ID(), blk.Time(), blk.LIBNum())
	if s.buffer != nil {
		if s.buffer.Len() >= s.bufferSize {
			s.buffer.Delete(s.buffer.Tail())
		}
		s.buffer.AppendHead(blk)
	}

	for _, sub := range s.subscriptions {
		if sub.closed {
			sub.logger.Info("not pushing block to a closed subscription")
			continue
		}
		sub.Push(blk)
	}

	return nil
}

func (s *Server) subscribe(requestedBurst int, subscriber string) *subscription {
	s.lock.Lock()
	defer s.lock.Unlock()

	chanSize := 200
	var blocks []bstream.BlockRef

	if s.buffer != nil {
		blocks = s.buffer.AllBlocks()

		if requestedBurst < len(blocks) {
			blocks = blocks[len(blocks)-requestedBurst:]
			chanSize += requestedBurst
		} else {
			chanSize += len(blocks)
		}
	}

	sub := newSubscription(chanSize, s.logger.Named("sub").Named(subscriber))

	sub.logger.Info("sending burst", zap.Int("busrt_size", len(blocks)))
	for _, blk := range blocks {
		if sub.closed {
			sub.logger.Info("subscription closed during burst", zap.Int("busrt_size", len(blocks)))
			return nil
		}
		sub.Push(blk.(*bstream.Block))
	}

	s.subscriptions = append(s.subscriptions, sub)
	s.logger.Info("subscribed", zap.Int("new_length", len(s.subscriptions)), zap.String("subscriber", subscriber))

	return sub
}

func (s *Server) unsubscribe(toRemove *subscription) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var newListeners []*subscription
	for _, sub := range s.subscriptions {
		if sub != toRemove {
			newListeners = append(newListeners, sub)
		}
	}

	s.subscriptions = newListeners
	s.logger.Info("unsubscribed", zap.Int("new_length", len(s.subscriptions)))
}
