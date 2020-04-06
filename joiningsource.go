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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dfuse-io/logging"

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbmerger "github.com/dfuse-io/pbgo/dfuse/merger/v1"
	"github.com/dfuse-io/shutter"
	"github.com/dfuse-io/dgrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// JoiningSource needs a buffer, in which to put the blocks from the liveSource until joined.
// as soon as the fileSource reads a block that is in the buffer, we:
//   1) shutdown the filesource and set livePassThru to true
//   2) process all the blocks from the buffer that are >= that joining block (with a lock...)
//   3) "delete" the buffer
//   4) configure the "incomingFromLive" handler to directly process the next blocks

var joiningSourceLogger *zap.Logger

func init() {
	logging.Register("github.com/dfuse-io/bstream/JoiningSource.go", &joiningSourceLogger)
}

type JoiningSource struct {
	*shutter.Shutter

	fileSourceFactory SourceFactory
	liveSourceFactory SourceFactory

	sourcesLock sync.Mutex
	handlerLock sync.Mutex

	liveSource     Source
	livePassThru   bool
	fileSource     Source
	mergerAddr     string
	targetBlockID  string
	targetBlockNum uint64

	handler                      Handler
	lastFileProcessedBlockID     string
	highestFileProcessedBlockNum uint64
	liveBuffer                   *Buffer
	liveBufferSize               int
	state                        *joinSourceState
	rateLimit                    func(counter int)
	rateLimiterCounter           int

	name string
}

type JoiningSourceOption = func(s *JoiningSource)

func NewJoiningSource(fileSourceFactory, liveSourceFactory SourceFactory, h Handler, options ...JoiningSourceOption) *JoiningSource {
	s := &JoiningSource{
		fileSourceFactory: fileSourceFactory,
		liveSourceFactory: liveSourceFactory,
		handler:           h,
		liveBuffer:        NewBuffer("joiningSource"),
		liveBufferSize:    300,
		name:              "default",
	}

	joiningSourceLogger.Info("Creating new joining source")
	for _, option := range options {
		option(s)
	}

	s.Shutter = shutter.New()
	s.Shutter.OnTerminating(func(err error) {
		zlogger := joiningSourceLogger.With(zap.String("name", s.name))

		s.sourcesLock.Lock()
		defer s.sourcesLock.Unlock()

		if s.fileSource != nil {
			zlogger.Debug("shutting down file source")
			s.fileSource.Shutdown(err)
		}
		if s.liveSource != nil {
			zlogger.Debug("shutting down live source")
			s.liveSource.Shutdown(err)
		}
	})

	return s
}

func (s *JoiningSource) SetName(name string) {
	s.name = name
}

// JoiningSourceTargetBlockID is an option for when we know right away
// the ID of the block where we want to start. In this case we'll accept that block coming
// from live stream right away (if we have not started processing blocks from file)
// it prevents waiting for a file to be merged when the live stream is serving us our startBlockID
// This is not recommended from a block number because we could have missed a "version" of that block
// number from the live source, but in a filesource, we always start from the first occurence of a blocknum.
func JoiningSourceTargetBlockID(id string) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.targetBlockID = id
	}
}

// JoiningSourceTargetBlockNum is like JoiningSourceTargetBlockID
// but allows starting immediately from block num == 2 on EOS or similar
// You better be DAMN SURE that you won't get a forked block at this number, beware
func JoiningSourceTargetBlockNum(num uint64) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.targetBlockNum = num
	}
}

func JoiningSourceName(name string) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.name = name
	}
}

func JoiningSourceRateLimit(rampLength int, sleepBetweenBlocks time.Duration) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.rateLimit = func(counter int) {
			if counter >= rampLength {
				time.Sleep(sleepBetweenBlocks)
			}

			sleepTime := sleepBetweenBlocks * time.Duration((counter*100/rampLength)/100)
			if sleepTime > 0 {
				time.Sleep(sleepTime)
			}
		}
	}
}

func JoiningSourceMergerAddr(mergerAddr string) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.mergerAddr = mergerAddr
	}
}

func (s *JoiningSource) Run() {
	s.Shutdown(s.run())
}

func (s *JoiningSource) run() error {
	joiningSourceLogger.Info("Joining Source is now running", zap.String("name", s.name))
	s.sourcesLock.Lock()

	s.state = &joinSourceState{}
	s.state.logd(s)

	if s.fileSourceFactory != nil {
		s.fileSource = s.fileSourceFactory(HandlerFunc(s.incomingFromFile))
	}

	if s.liveSourceFactory != nil {
		s.liveSource = s.liveSourceFactory(HandlerFunc(s.incomingFromLive))
	}

	_ = s.LockedInit(func() error {
		if s.fileSource != nil {
			s.fileSource.OnTerminating(func(err error) {
				if err := s.fileSource.Err(); err != nil {
					s.Shutdown(fmt.Errorf("file source failed: %s", err))
				}
				if !s.livePassThru {
					s.Shutdown(fmt.Errorf("file source was shut down and we're not live"))
				}
			})

			if s.mergerAddr != "" {
				fs, ok := s.fileSource.(*FileSource)
				if !ok {
					panic(fmt.Errorf("cannot call SetNotFoundCallback on a non-filesource instance, received a filesource of type %T", s.fileSource))
				}

				fs.SetNotFoundCallback(func(blockNum uint64) {
					liveBuffer := s.liveBuffer
					if liveBuffer == nil { // the joining is done, liveBuffer is now set to nil
						return
					}
					targetJoinBlock := lowestIDInBufferGTE(blockNum, liveBuffer)
					if targetJoinBlock == nil {
						return
					}

					if s.highestFileProcessedBlockNum != 0 && s.highestFileProcessedBlockNum != blockNum-1 {
						joiningSourceLogger.Debug("skipping asking merger because we haven't received previous file yet, this would create a gap")
						return
					}
					src := newFromMergerSource(
						joiningSourceLogger.With(zap.String("name", fmt.Sprintf("%s-merger-%d", s.name, blockNum))),
						blockNum,
						targetJoinBlock.ID(),
						s.mergerAddr,
						HandlerFunc(s.incomingFromMerger),
					)

					if src == nil {
						return
					}

					src.Run()

					// WARN: access of `livePassThru` isn't locked
					if !s.livePassThru {
						err := errors.New("joining source is not live after processing blocks from merger")
						if src.Err() != nil {
							err = fmt.Errorf("%s: %s", err, src.Err())
						}

						s.Shutdown(err)
					}
				})
			}

			go s.fileSource.Run()
		}

		if s.liveSource != nil {
			s.liveSource.OnTerminating(func(err error) {
				if err != nil {
					s.Shutdown(fmt.Errorf("live source faild: %s", err))
				} else {
					s.Shutdown(nil)
				}
			})

			joiningSourceLogger.Info("Joining Source: calling run on live source", zap.String("name", s.name))
			go s.liveSource.Run()
		}

		return nil
	})

	s.sourcesLock.Unlock()

	<-s.Terminating()

	return s.Err()
}

func lowestIDInBufferGTE(blockNum uint64, buf *Buffer) (blk BlockRef) {
	for _, blk := range buf.AllBlocks() {
		if blk.Num() < blockNum {
			continue
		}
		return blk
	}
	return nil
}

func newFromMergerSource(zlogger *zap.Logger, blockNum uint64, blockID string, mergerAddr string, handler Handler) Source {
	joiningSourceLogger.Info("creating merger source due to filesource file not found callback", zap.Uint64("file_not_found_base_block_num", blockNum))
	conn, err := dgrpc.NewInternalClient(mergerAddr)

	client := pbmerger.NewMergerClient(conn)
	resp, err := client.PreMergedBlocks(context.Background(), &pbmerger.Request{
		LowBlockNum: blockNum,
		HighBlockID: blockID,
	}, grpc.MaxCallRecvMsgSize(50*1024*1024*1024), grpc.WaitForReady(false))

	if err != nil {
		zlogger.Info("got error from PreMergedBlocks call, merger source will not be used", zap.Error(err))
		return nil
	}

	if !resp.Found || len(resp.Blocks) == 0 {
		zlogger.Info("received not found response from PreMergedBlocks call, merger source will not be used")
		return nil
	}

	zlogger.Info("received found response from PreMergedBlocks call, merger source will be used", zap.Int("block_count", len(resp.Blocks)), zap.Uint64("low_block_number", resp.Blocks[0].Number))
	return newArraySource(resp.Blocks, handler)

}

type arraySource struct {
	*shutter.Shutter
	blocks  []*pbbstream.Block
	handler Handler
}

func newArraySource(blocks []*pbbstream.Block, h Handler) *arraySource {
	return &arraySource{
		blocks:  blocks,
		handler: h,
		Shutter: shutter.New(),
	}
}

func (s *arraySource) Run() {
	for _, blk := range s.blocks {
		nativeBlock, err := BlockFromProto(blk)
		if err != nil {
			s.Shutdown(err)
		}
		s.handler.ProcessBlock(nativeBlock, nil)
	}

	s.Shutdown(nil)
}

func (s *JoiningSource) incomingFromFile(blk *Block, obj interface{}) error {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()

	if s.IsTerminating() {
		return fmt.Errorf("not processing blocks when down")
	}

	if s.livePassThru {
		return fmt.Errorf("fileSource should be shut down, incomingFromFile should not be called")
	}

	s.state.lastFileBlock = blk.Num()
	if s.liveBuffer.Exists(blk.ID()) {
		s.livePassThru = true
		err := s.processLiveBuffer(blk)
		if err != nil {
			return err
		}

		joiningSourceLogger.Info("shutting file source, switching to live (from a file block matching)", zap.String("name", s.name))

		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return nil
	}
	if s.rateLimit != nil {
		s.rateLimit(s.rateLimiterCounter)
		s.rateLimiterCounter++
	}

	if blk.Num() > s.highestFileProcessedBlockNum {
		s.highestFileProcessedBlockNum = blk.Num()
	}
	s.lastFileProcessedBlockID = blk.ID()
	joiningSourceLogger.Debug("processing from file", zap.Uint64("block_num", blk.Num()))
	return s.handler.ProcessBlock(blk, obj)

}
func (s *JoiningSource) incomingFromMerger(blk *Block, obj interface{}) error {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()

	if s.IsTerminating() {
		return fmt.Errorf("not processing blocks when down")
	}
	if s.livePassThru {
		return fmt.Errorf("file source should be shut down, incomingFromFile should not be called")
	}

	s.state.lastMergerBlock = blk.Num()
	if s.liveBuffer.Exists(blk.ID()) {
		s.livePassThru = true
		err := s.processLiveBuffer(blk)
		if err != nil {
			return err
		}

		joiningSourceLogger.Info("shutting file source, switching to live (from a merger block matching)", zap.String("name", s.name))

		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return nil
	}

	s.lastFileProcessedBlockID = blk.ID()
	joiningSourceLogger.Debug("processing from merger", zap.Uint64("block_num", blk.Num()))
	return s.handler.ProcessBlock(blk, obj)
}

func (s *JoiningSource) incomingFromLive(blk *Block, obj interface{}) error {
	s.handlerLock.Lock()
	defer s.handlerLock.Unlock()

	if s.IsTerminating() {
		return fmt.Errorf("not processing blocks when down")
	}

	s.state.lastLiveBlock = blk.Num()
	if s.livePassThru {
		joiningSourceLogger.Debug("processing from live", zap.Uint64("block_num", blk.Num()))
		return s.handler.ProcessBlock(blk, obj)
	}

	if s.lastFileProcessedBlockID == blk.ID() {
		s.livePassThru = true
		joiningSourceLogger.Info("shutting file source, switching to live (from a live block matching)", zap.String("name", s.name), zap.Stringer("block", blk))
		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return nil
	}

	if s.targetBlockNum != 0 && blk.Num() == s.targetBlockNum && s.lastFileProcessedBlockID == "" {
		s.livePassThru = true
		joiningSourceLogger.Info("shutting file source, starting from live at requested block ID", zap.String("name", s.name), zap.Stringer("block", blk))
		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return s.handler.ProcessBlock(blk, obj)
	}

	if s.targetBlockID != "" && blk.ID() == s.targetBlockID && s.lastFileProcessedBlockID == "" {
		s.livePassThru = true
		joiningSourceLogger.Info("shutting file source, starting from live at requested block ID", zap.String("name", s.name), zap.Stringer("block", blk))
		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return s.handler.ProcessBlock(blk, obj)
	}

	if s.liveBuffer.Len() >= s.liveBufferSize {
		s.liveBuffer.Delete(s.liveBuffer.Tail())
	}
	s.liveBuffer.AppendHead(&PreprocessedBlock{Block: blk, Obj: obj})
	return nil
}

func (s *JoiningSource) processLiveBuffer(liveBlock *Block) (err error) {
	liveID := liveBlock.ID()
	joiningSourceLogger.Debug("looking for ID", zap.String("live_id", liveID), zap.Uint64("live_num", liveBlock.Num()))
	gatePassed := false
	count := 0
	for _, blk := range s.liveBuffer.AllBlocks() {
		blk, ok := blk.(*PreprocessedBlock)
		if !ok {
			panic("buffer contained non-block object")
		}

		if blk.ID() == liveID {
			gatePassed = true
		}

		if gatePassed {
			count += 1
			joiningSourceLogger.Debug("processing from live buffer", zap.Uint64("block_num", blk.Num()))
			if err = s.handler.ProcessBlock(blk.Block, blk.Obj); err != nil {
				return err
			}
		}
	}
	joiningSourceLogger.Debug("finished processing liveBuffer", zap.Bool("gatePassed", gatePassed), zap.Int("count", count), zap.Int("len_livebuffer", len(s.liveBuffer.AllBlocks())), zap.Bool("exists_in_livebuffer", s.liveBuffer.Exists(liveID)))
	return nil
}

type joinSourceState struct {
	lastFileBlock   uint64
	lastMergerBlock uint64
	lastLiveBlock   uint64
}

func (s *joinSourceState) logd(joiningSource *JoiningSource) {
	go func() {
		zlogger := joiningSourceLogger.With(zap.String("name", joiningSource.name))
		seenLive := false

		for {
			if joiningSource.IsTerminating() {
				return
			}

			if joiningSource.livePassThru {
				if seenLive {
					zlogger.Debug("joining state LIVE",
						zap.Uint64("last_live_block", s.lastLiveBlock),
					)
				} else {
					zlogger.Info("joining state LIVE",
						zap.Uint64("last_live_block", s.lastLiveBlock),
					)
					seenLive = true
				}
				time.Sleep(30 * time.Second)
			} else {
				var tailNum, headNum uint64
				if tail := joiningSource.liveBuffer.Tail(); tail != nil {
					tailNum = tail.Num()
				}
				if head := joiningSource.liveBuffer.Head(); head != nil {
					headNum = head.Num()
				}
				zlogger.Info("joining state JOINING",
					zap.Uint64("block_behind_live", (s.lastLiveBlock-s.lastFileBlock)),
					zap.Uint64("last_file_block", s.lastFileBlock),
					zap.Uint64("last_live_block", s.lastLiveBlock),
					zap.Uint64("last_merger_block", s.lastMergerBlock),
					zap.Uint64("buffer_lower_block", tailNum),
					zap.Uint64("buffer_higher_block", headNum),
				)
				time.Sleep(5 * time.Second)
			}
		}
	}()
}
