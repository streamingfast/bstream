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
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dgrpc"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Source struct {
	*shutter.Shutter

	ctx            context.Context
	endpointURL    string
	burst          int64
	handler        bstream.Handler
	preprocFunc    bstream.PreprocessFunc
	preprocThreads int
	gator          bstream.Gator

	requester string
	logger    *zap.Logger
}

type SourceOption = func(s *Source)

func WithRequester(requester string) SourceOption {
	return func(s *Source) {
		s.requester = requester
	}
}

func WithLogger(logger *zap.Logger) SourceOption {
	return func(s *Source) {
		s.logger = logger
	}
}

func WithTimeThresholdGator(threshold time.Duration) SourceOption {
	return func(s *Source) {
		s.logger.Info("setting time gator", zap.Duration("threshold", threshold))
		s.gator = bstream.NewTimeThresholdGator(threshold)
	}
}

func WithNumGator(blockNum uint64, exclusive bool) SourceOption {
	return func(s *Source) {
		s.logger.Info("setting num gator", zap.Uint64("block_num", blockNum), zap.Bool("exclusive", exclusive))
		if exclusive {
			s.gator = bstream.NewExclusiveBlockNumberGator(blockNum)
		} else {
			s.gator = bstream.NewBlockNumberGator(blockNum)
		}
	}
}

func WithParallelPreproc(f bstream.PreprocessFunc, threads int) SourceOption {
	return func(s *Source) {
		s.preprocFunc = f
		s.preprocThreads = threads
	}
}

func NewSource(
	ctx context.Context,
	endpointURL string,
	burst int64,
	h bstream.Handler,
	options ...SourceOption,
) *Source {
	s := &Source{
		ctx:         ctx,
		endpointURL: endpointURL,
		burst:       burst,
		handler:     h,
		Shutter:     shutter.New(),
		logger:      zlog,
	}

	for _, option := range options {
		option(s)
	}
	s.logger = s.logger.With(zap.String("endpoint_url", s.endpointURL))

	return s
}

func (s *Source) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *Source) SetParallelPreproc(f bstream.PreprocessFunc, threads int) {
	s.preprocFunc = f
	s.preprocThreads = threads
}

func (s *Source) Run() {
	var transport *grpc.ClientConn
	err := s.LockedInit(func() error {
		var err error
		transport, err = dgrpc.NewInternalClient(s.endpointURL)
		if err != nil {
			return err
		}

		s.OnTerminating(func(_ error) {
			if err := transport.Close(); err != nil {
				s.logger.Info("failed closing client transport on shutdown", zap.Error(err))
			}
		})

		return nil
	})
	if err != nil {
		s.Shutdown(err)
		return
	}

	client := pbbstream.NewBlockStreamClient(transport)

	s.Shutdown(s.run(client))
}

func (s *Source) run(client pbbstream.BlockStreamClient) (err error) {
	s.logger.Debug("source connecting")
	blocksStreamer, err := client.Blocks(s.ctx, &pbbstream.BlockRequest{
		Burst:     s.burst,
		Requester: s.requester,
	})
	if err != nil {
		return fmt.Errorf("failed to strart block source streamer: %w", err)
	}

	s.logger.Info("starting block source consumption")
	s.readStream(blocksStreamer)
	s.logger.Info("source shutting down", zap.Error(s.Err()))

	return s.Err()
}

func (s *Source) readStream(client pbbstream.BlockStream_BlocksClient) {
	s.logger.Info("block stream source reading messages")

	blkchan := make(chan chan *bstream.PreprocessedBlock, s.preprocThreads)
	go func() {
		for {
			response, err := client.Recv()
			if err != nil {
				s.Shutdown(err)
				return
			}

			blk, err := bstream.NewBlockFromProto(response)
			if err != nil {
				s.Shutdown(fmt.Errorf("unable to transform to bstream.Block: %w", err))
				return
			}

			if s.gator != nil && !s.gator.Pass(blk) {
				s.logger.Debug("gator not passed dropping block")
				continue
			}

			singleBlockChan := make(chan *bstream.PreprocessedBlock)
			go func() {
				var obj interface{}
				var err error
				if s.preprocFunc != nil {
					obj, err = s.preprocFunc(blk)
					if err != nil {
						s.Shutdown(err)
						close(singleBlockChan)
						return
					}
				}
				select {
				case singleBlockChan <- &bstream.PreprocessedBlock{
					Block: blk,
					Obj:   obj,
				}:
				case <-s.Terminating():
				}
			}()
			select {
			case <-s.Terminating():
				return
			case blkchan <- singleBlockChan:
			}
		}
	}()

	for {
		select {
		case <-s.Terminating():
			return
		case singleBlockChan := <-blkchan:
			select {
			case <-s.Terminating():
				return
			case ppblk, ok := <-singleBlockChan:
				if s.IsTerminating() {
					return
				}
				if !ok {
					s.Shutdown(fmt.Errorf("preprocess channel closed"))
					return
				}
				if err := s.handler.ProcessBlock(ppblk.Block, ppblk.Obj); err != nil {
					s.Shutdown(err)
					return
				}

			}
		}
	}
}
