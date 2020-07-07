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

package trxstream

import (
	"net"
	"sync"

	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ServerOption func(s *Server)

func ServerOptionWithLogger(logger *zap.Logger) ServerOption {
	return func(s *Server) {
		s.logger = logger
	}
}

type Server struct {
	subscriptions []*subscription
	grpcServer    *grpc.Server
	lock          sync.RWMutex

	logger *zap.Logger
}

func NewServer(server *grpc.Server, opts ...ServerOption) *Server {
	s := &Server{
		grpcServer: server,
		logger:     zlog,
	}

	for _, opt := range opts {
		opt(s)
	}

	pbbstream.RegisterTransactionStreamServer(s.grpcServer, s)
	return s
}

func (s *Server) Transactions(r *pbbstream.TransactionRequest, stream pbbstream.TransactionStream_TransactionsServer) error {
	zlogger := logging.Logger(stream.Context(), s.logger)

	subscription := s.subscribe(zlogger)
	defer s.unsubscribe(subscription)

	for {
		select {
		// FIXME (MATT): We need to handle the case where the subscription directly closed the incoming block channel
		case <-stream.Context().Done():
			return nil
		case trx, ok := <-subscription.incomingTrx:
			if !ok {
				// we've been shutdown somehow, simply close the current connection..
				// we'll have logged at the source
				return nil
			}
			zlogger.Debug("sending transaction to subscription", zap.Stringer("transaction", trx))
			err := stream.Send(trx)
			if err != nil {
				zlogger.Info("failed writing to socket, shutting down subscription", zap.Error(err))
				break
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
	return true
}

func (s *Server) PushTransaction(trx *pbbstream.Transaction) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, sub := range s.subscriptions {
		sub.Push(trx)
	}

	return
}

func (s *Server) subscribe(logger *zap.Logger) *subscription {
	chanSize := 200
	sub := newSubscription(chanSize, s.logger.Named("sub"))

	s.lock.Lock()
	defer s.lock.Unlock()

	s.subscriptions = append(s.subscriptions, sub)
	s.logger.Info("subscribed", zap.Int("new_length", len(s.subscriptions)))

	return sub
}

func (s *Server) unsubscribe(toRemove *subscription) {
	var newListeners []*subscription
	for _, sub := range s.subscriptions {
		if sub != toRemove {
			newListeners = append(newListeners, sub)
		}
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.subscriptions = newListeners
	s.logger.Info("unsubscribed", zap.Int("new_length", len(s.subscriptions)))
}
