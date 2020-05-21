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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
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

// DumbStartBlockResolver will help you start x blocks before your target start block
func DumbStartBlockResolver(precedingBlocks uint64) StartBlockResolverFunc {
	return func(_ context.Context, targetBlockNum uint64) (uint64, string, error) {
		if targetBlockNum <= precedingBlocks {
			return 0, "", nil
		}
		return targetBlockNum - precedingBlocks, "", nil
	}
}

// ParallelStartResolver will call multiple resolvers to get the fastest answer. It retries each resolver 'attempts' time before bailing out. If attempts<0, it will retry forever.
func ParallelStartResolver(resolvers []StartBlockResolver, attempts int) StartBlockResolverFunc {
	return func(ctx context.Context, targetStartBlockNum uint64) (uint64, string, error) {
		childrenCtx, cancelChildren := context.WithCancel(ctx)
		defer cancelChildren()

		outChan := make(chan *resolveStartBlockResp)
		for _, resolver := range resolvers {
			go attemptResolveStartBlock(childrenCtx, targetStartBlockNum, resolver, attempts, outChan)
		}

		var allErrors []error
		for cnt := 0; cnt < len(resolvers); cnt++ {
			select {
			case <-ctx.Done():
				return 0, "", ctx.Err()
			case resp := <-outChan:
				if resp.errs == nil {
					return resp.startBlockNum, resp.previousIrreversibleID, nil
				}
				allErrors = append(allErrors, resp.errs...)
			}
		}

		return 0, "", fmt.Errorf("errors during attempts to each resolver: %s", allErrors)
	}
}

type resolveStartBlockResp struct {
	startBlockNum          uint64
	previousIrreversibleID string
	errs                   []error
}

func attemptResolveStartBlock(ctx context.Context, targetStartBlockNum uint64, resolver StartBlockResolver, attempts int, outChan chan *resolveStartBlockResp) {
	out := &resolveStartBlockResp{}
	var errs []error

	for attempt := 0; attempts < 0 || attempt <= attempts; attempt++ {
		s, p, e := resolver.Resolve(ctx, targetStartBlockNum)
		if e == nil {
			zlog.Debug("resolved start block num", zap.Uint64("target_start_block_num", targetStartBlockNum), zap.Uint64("start_block_num", s), zap.String("previous_irreversible_id", p))
			out.startBlockNum = s
			out.previousIrreversibleID = p

			select {
			case outChan <- out:
			case <-ctx.Done():
			}
			return
		}

		if ctx.Err() != nil {
			return
		}

		zlog.Debug("got an error from a block resolver", zap.Error(e))
		errs = append(errs, e)
		attempt++
		time.Sleep(time.Second)
	}
	out.errs = errs
	select {
	case outChan <- out:
	case <-ctx.Done():
	}
	return

}
