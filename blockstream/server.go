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
	"net"
	"sync"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbheadinfo "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	headInfo      *headInfo
	buffer        *bstream.Buffer
	bufferSize    int
	subscriptions []*subscription
	grpcServer    *grpc.Server

	lock sync.RWMutex
}

func NewBufferedServer(server *grpc.Server, size int) *Server {
	bs := NewServer(server)
	bs.buffer = bstream.NewBuffer("blockserver")
	bs.bufferSize = size
	return bs
}

func NewServer(server *grpc.Server) *Server {
	s := &Server{
		grpcServer: server,
	}

	pbheadinfo.RegisterHeadInfoServer(s.grpcServer, s)
	pbbstream.RegisterBlockStreamServer(s.grpcServer, s)
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

	resp := &pbheadinfo.HeadInfoResponse{
		LibNum:   s.headInfo.libNum,
		HeadNum:  s.headInfo.Num,
		HeadID:   s.headInfo.ID,
		HeadTime: s.headInfo.time,
	}
	return resp, nil
}

func (s *Server) Blocks(r *pbbstream.BlockRequest, stream pbbstream.BlockStream_BlocksServer) error {
	zlog.Info("receive block request", zap.String("requester", r.Requester), zap.Reflect("request", r))
	subscription := s.subscribe(int(r.Burst), r.Requester)
	if subscription == nil {
		return fmt.Errorf("failed to create subscription for subscriber: %s", r.Requester)
	}
	defer s.unsubscribe(subscription)

	zlogger := logging.Logger(stream.Context(), zlog)

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

			zlog.Debug("sending block to subscription", zap.Stringer("block", blk))
			block, err := blk.ToProto()
			if err != nil {
				panic(fmt.Errorf("unable to transform from bstream.Block to StreamableBlock: %s", err))
			}

			err = stream.Send(block)
			zlog.Debug("block sent to stream", zap.Stringer("block", blk))
			if err != nil {
				zlogger.Info("failed writing to socket, shutting down subscription", zap.Error(err))
				return nil
			}
		}
	}
}

func (s *Server) Serve(listener net.Listener) error {
	return s.grpcServer.Serve(listener)
}

func (s *Server) Close() {
	s.grpcServer.Stop()
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
			zlog.Info("not pushing block to a closed subscription", zap.String("subscriber", sub.subscriber))
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

	sub := newSubscription(chanSize)
	sub.SetSubscriber(subscriber)

	zlog.Info("sending burst", zap.Int("busrt_size", len(blocks)), zap.String("subscriber", subscriber))

	for _, blk := range blocks {
		if sub.closed {
			zlog.Info("subscription closed during burst", zap.Int("busrt_size", len(blocks)), zap.String("subscriber", subscriber))
			return nil
		}
		sub.Push(blk.(*bstream.Block))
	}

	s.subscriptions = append(s.subscriptions, sub)
	zlog.Info("subscribed", zap.Int("new_length", len(s.subscriptions)), zap.String("subscriber", subscriber))

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
	zlog.Info("unsubscribed", zap.Int("new_length", len(s.subscriptions)))
}
