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

	"github.com/streamingfast/dgrpc"
	pbmerger "github.com/streamingfast/pbgo/dfuse/merger/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// JoiningSource needs a buffer, in which to put the blocks from the liveSource until joined.
// as soon as the fileSource reads a block that is in the buffer, we:
//   1) shutdown the filesource and set livePassThru to true
//   2) process all the blocks from the buffer that are >= that joining block (with a lock...)
//   3) "delete" the buffer
//   4) configure the "incomingFromLive" handler to directly process the next blocks

type JoiningSource struct {
	*shutter.Shutter

	fileSourceFactory SourceFactory
	liveSourceFactory SourceFactory

	sourcesLock sync.Mutex
	handlerLock sync.Mutex

	liveSource           Source
	livePassThru         bool
	fileSource           Source
	mergerAddr           string
	targetBlockID        string
	targetBlockNum       uint64
	startLiveImmediately *bool

	tracker        *Tracker
	trackerTimeout time.Duration

	handler                      Handler
	lastFileProcessedBlock       *Block
	highestFileProcessedBlockNum uint64

	liveBuffer         *Buffer
	liveBufferSize     int
	state              joinSourceState
	rateLimit          func(counter int)
	rateLimiterCounter int

	logger *zap.Logger
}

type JoiningSourceOption = func(s *JoiningSource)

func NewJoiningSource(fileSourceFactory, liveSourceFactory SourceFactory, h Handler, options ...JoiningSourceOption) *JoiningSource {
	s := &JoiningSource{
		fileSourceFactory: fileSourceFactory,
		liveSourceFactory: liveSourceFactory,
		handler:           h,
		liveBufferSize:    300,
		trackerTimeout:    6 * time.Second,
		logger:            zlog,
	}

	for _, option := range options {
		option(s)
	}

	// Created after option so logger is set when called
	s.liveBuffer = NewBuffer("joiningSource", s.logger.Named("buffer"))

	s.logger.Info("creating new joining source")
	s.Shutter = shutter.New()
	s.Shutter.OnTerminating(func(err error) {
		s.sourcesLock.Lock()
		defer s.sourcesLock.Unlock()

		if s.fileSource != nil {
			s.logger.Debug("shutting down file source")
			s.fileSource.Shutdown(err)
		}
		if s.liveSource != nil {
			s.logger.Debug("shutting down live source")
			s.liveSource.Shutdown(err)
		}
	})

	return s
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

func JoiningSourceLiveTracker(nearBlocksCount uint64, liveHeadGetter BlockRefGetter) JoiningSourceOption {
	// most of the time, use `bstream.HeadBlockRefGetter(headinfoAddr)` as `liveHeadGetter`.
	return func(s *JoiningSource) {
		s.tracker = NewTracker(nearBlocksCount)
		s.tracker.AddGetter(FileSourceHeadTarget, s.LastFileBlockRefGetter)
		s.tracker.AddGetter(LiveSourceHeadTarget, liveHeadGetter)
	}
}

func JoiningSourceLogger(logger *zap.Logger) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.logger = logger.Named("js")
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

// JoiningSourceStartLiveImmediately sets the behavior to override waiting for the tracker before starting the live source
func JoiningSourceStartLiveImmediately(b bool) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.startLiveImmediately = &b
	}
}

func JoiningSourceMergerAddr(mergerAddr string) JoiningSourceOption {
	return func(s *JoiningSource) {
		s.mergerAddr = mergerAddr
	}
}

func (s *JoiningSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *JoiningSource) Run() {
	s.Shutdown(s.run())
}

func (s *JoiningSource) run() error {
	s.logger.Info("joining Source is now running")
	s.sourcesLock.Lock()

	s.state = joinSourceState{logger: s.logger}
	s.state.logd(s)

	if s.fileSourceFactory != nil {
		s.fileSource = s.fileSourceFactory(HandlerFunc(s.incomingFromFile))
		s.fileSource.SetLogger(s.logger.Named("file"))
	}

	if s.liveSourceFactory != nil {
		s.liveSource = s.liveSourceFactory(HandlerFunc(s.incomingFromLive))
		s.liveSource.SetLogger(s.logger.Named("live"))
	}

	_ = s.LockedInit(func() error {
		if s.fileSource != nil {
			s.fileSource.OnTerminating(func(err error) {
				if err := s.fileSource.Err(); err != nil {
					s.Shutdown(fmt.Errorf("file source failed: %w", err))
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
						s.logger.Debug("skipping asking merger because we haven't received previous file yet, this would create a gap")
						return
					}
					src, err := newFromMergerSource(
						s.logger.Named("merger"),
						blockNum,
						targetJoinBlock.ID(),
						s.mergerAddr,
						HandlerFunc(s.incomingFromMerger),
					)
					if err != nil {
						s.logger.Info("cannot join using merger source", zap.Error(err), zap.Stringer("target_join_block", targetJoinBlock))
						return
					}

					s.logger.Info("launching source from merger", zap.Uint64("low_block_num", blockNum), zap.Stringer("target_join_block", targetJoinBlock))

					src.Run()

					// WARN: access of `livePassThru` isn't locked
					if !s.livePassThru {
						err := errors.New("joining source is not live after processing blocks from merger")
						if src.Err() != nil {
							err = fmt.Errorf("%s: %w", err, src.Err())
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
					s.Shutdown(fmt.Errorf("live source failed: %w", err))
				} else {
					s.Shutdown(nil)
				}
			})

			if s.startLiveImmediately == nil {
				targetBlockIDSet := s.targetBlockID != "" // allows quick reconnection from eternal source: default behavior
				s.startLiveImmediately = &targetBlockIDSet
			}

			go func() {
				for s.tracker != nil && !*s.startLiveImmediately {
					if s.IsTerminating() { // no more need to start live if joiningSource is shut down
						return
					}

					ctx, cancel := context.WithTimeout(context.Background(), s.trackerTimeout)
					fileBlock, liveBlock, near, err := s.tracker.IsNearWithResults(ctx, FileSourceHeadTarget, LiveSourceHeadTarget)
					if err == nil && near {
						zlog.Debug("tracker near, starting live source")
						cancel()
						break
					}

					// manually checking nearness to targetBlockNum if not zero
					if liveBlock != nil {
						s.handlerLock.Lock()
						s.state.lastLiveBlock = liveBlock.Num()
						s.handlerLock.Unlock()
						if s.targetBlockNum != 0 && s.tracker.IsNearManualCheck(s.targetBlockNum, liveBlock.Num()) {
							s.logger.Debug("tracker near 'targetBlockNum', starting live source")
							cancel()
							break
						}
					}

					zlog.Debug("tracker returned not ready", zap.Error(err))
					if fileBlock == nil || fileBlock.Num() == 0 {
						time.Sleep(200 * time.Millisecond)
						cancel()
					}
					<-ctx.Done()
					cancel()
					continue
				}
				s.logger.Debug("calling run on live source")
				s.liveSource.Run()
			}()
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

func newFromMergerSource(logger *zap.Logger, blockNum uint64, blockID string, mergerAddr string, handler Handler) (*preMergeBlockSource, error) {
	conn, err := dgrpc.NewInternalClient(mergerAddr)
	if err != nil {
		return nil, err
	}

	client := pbmerger.NewMergerClient(conn)
	stream, err := client.PreMergedBlocks(
		context.Background(),
		&pbmerger.Request{
			LowBlockNum: blockNum,
			HighBlockID: blockID,
		},
		grpc.WaitForReady(false),
	)
	if err != nil {
		return nil, err
	}

	header, err := stream.Header()
	if err != nil {
		return nil, err
	}
	// we return failure to obtain blocks inside GRPC header
	if errmsgs := header.Get("error"); len(errmsgs) > 0 {
		return nil, fmt.Errorf("%s", errmsgs[0])
	}

	return newPreMergeBlockSource(stream, handler, logger), nil
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

		s.logger.Info("shutting file source, switching to live (from a file block matching)")

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
	s.lastFileProcessedBlock = blk
	s.logger.Debug("processing from file", zap.Stringer("block_num", blk))
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

		s.logger.Info("shutting file source, switching to live (from a merger block matching)")

		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return nil
	}

	s.lastFileProcessedBlock = blk
	s.logger.Debug("processing from merger", zap.Stringer("block_num", blk))
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
		if traceEnabled {
			s.logger.Debug("processing from live", zap.Stringer("block", blk))
		} else if blk.Number%600 == 0 {
			s.logger.Debug("processing from live (1/600 sampling)", zap.Stringer("block", blk))
		}

		return s.handler.ProcessBlock(blk, obj)
	}

	if s.lastFileProcessedBlock.ID() == blk.ID() {
		s.livePassThru = true
		s.logger.Info("shutting file source, switching to live (from a live block matching)", zap.Stringer("block", blk))
		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return nil
	}

	if s.targetBlockNum != 0 && blk.Num() == s.targetBlockNum && s.lastFileProcessedBlock == nil {
		s.livePassThru = true
		s.logger.Info("shutting file source, starting from live at requested block ID", zap.Stringer("block", blk))
		s.fileSource.Shutdown(nil)
		s.liveBuffer = nil
		return s.handler.ProcessBlock(blk, obj)
	}

	if s.targetBlockID != "" && blk.ID() == s.targetBlockID && s.lastFileProcessedBlock == nil {
		s.livePassThru = true
		s.logger.Info("shutting file source, starting from live at requested block ID", zap.Stringer("block", blk))
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

func (s *JoiningSource) LastFileBlockRefGetter(_ context.Context) (BlockRef, error) {
	// TODO: lock if needed
	if s.lastFileProcessedBlock != nil {
		return s.lastFileProcessedBlock, nil
	}
	return nil, ErrTrackerBlockNotFound
}

func (s *JoiningSource) processLiveBuffer(liveBlock *Block) (err error) {
	liveID := liveBlock.ID()
	s.logger.Debug("looking for ID", zap.String("live_id", liveID), zap.Uint64("live_num", liveBlock.Num()))
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
			s.logger.Debug("processing from live buffer", zap.Stringer("block", blk))
			if err = s.handler.ProcessBlock(blk.Block, blk.Obj); err != nil {
				return err
			}
		}
	}
	s.logger.Debug("finished processing liveBuffer", zap.Bool("gatePassed", gatePassed), zap.Int("count", count), zap.Int("len_livebuffer", len(s.liveBuffer.AllBlocks())), zap.Bool("exists_in_livebuffer", s.liveBuffer.Exists(liveID)))
	return nil
}

type joinSourceState struct {
	lastFileBlock   uint64
	lastMergerBlock uint64
	lastLiveBlock   uint64
	logger          *zap.Logger
}

func (s *joinSourceState) logd(joiningSource *JoiningSource) {
	go func() {
		seenLive := false
		time.Sleep(2 * time.Second) // start logging after we've had a chance to connect
		for {
			if joiningSource.IsTerminating() {
				return
			}

			if joiningSource.livePassThru {
				if seenLive {
					s.logger.Debug("joining state LIVE", zap.Uint64("last_live_block", s.lastLiveBlock))
				} else {
					s.logger.Info("joining state LIVE", zap.Uint64("last_live_block", s.lastLiveBlock))
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
				s.logger.Info("joining state JOINING",
					zap.Int64("block_behind_live", int64(s.lastLiveBlock)-int64(s.lastFileBlock)),
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
