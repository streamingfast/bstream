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

package bstream

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"

	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	pbmerger "github.com/streamingfast/pbgo/sf/merger/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// DoForProtocol extra the worker (a lambda) that will be invoked based on the
// received `kind` parameter. If the mapping exists, the worker is invoked and
// the error returned with the call. If the mapping does not exist, an error
// is returned. In all other cases, this function returns `nil`.
func DoForProtocol(kind pbbstream.Protocol, mappings map[pbbstream.Protocol]func() error) error {
	if worker, exists := mappings[kind]; exists {
		return worker()
	}

	return fmt.Errorf("don't know how to handle block kind %s", kind)
}

// MustDoForProtocol perform the same work, but accept only non-error
// lambdas as the worker and an inexistant mapping will panic.
func MustDoForProtocol(kind pbbstream.Protocol, mappings map[pbbstream.Protocol]func()) {
	if worker, exists := mappings[kind]; exists {
		worker()
		return
	}

	panic(fmt.Errorf("don't know how to handle block kind %s", kind))
}

// toBlockNum extracts the block number (or height) from a hex-encoded block ID.
func toBlockNum(blockID string) uint64 {
	if len(blockID) < 8 {
		return 0
	}
	bin, err := hex.DecodeString(blockID[:8])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(bin)
}

type preMergeBlockSource struct {
	*shutter.Shutter
	handler Handler
	logger  *zap.Logger
	stream  pbmerger.Merger_PreMergedBlocksClient
}

func newPreMergeBlockSource(stream pbmerger.Merger_PreMergedBlocksClient, h Handler, logger *zap.Logger) *preMergeBlockSource {
	return &preMergeBlockSource{
		stream:  stream,
		handler: h,
		Shutter: shutter.New(),
		logger:  logger,
	}
}

func (s *preMergeBlockSource) Run() {
	for {
		resp, err := s.stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			s.logger.Info("error receiving message from merger pre merger block stream", zap.Error(err))
			s.Shutter.Shutdown(err)
			return
		}

		s.logger.Debug("receive pre merge block", zap.Uint64("block_num", resp.Block.Number), zap.String("block_id", resp.Block.Id))
		nativeBlock, err := NewBlockFromProto(resp.Block)
		if err != nil {
			s.Shutdown(err)
		}
		err = s.handler.ProcessBlock(nativeBlock, nil)
		s.Shutdown(err)
	}

	s.Shutdown(nil)
}

func (s *preMergeBlockSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func lowBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod)
}
