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
	"sync"

	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// JoiningSource joins an irreversible-only source (left) to a fork-aware source, close to HEAD (right)
// 1) it tries to get the source from RightSourceFactory (using startblock or cursor)
// 2) if it can't, it will ask the LeftSourceFactory for a source of those blocks.
// 3) when it receives blocks from LeftSource, it looks at RightSource
// the JoiningSource will instantiate and run an 'initialSource' until it can bridge the gap
type JoiningSource struct {
	*shutter.Shutter

	leftSourceFactory  ForkableSourceFactory
	rightSourceFactory ForkableSourceFactory

	leftSource  Source
	rightSource Source
	sourcesLock sync.Mutex

	handler     Handler
	handlerLock sync.Mutex

	lastBlockProcessed *Block

	irreversibleStartBlockNum uint64  // mutually exclusive with cursor
	cursor                    *Cursor // mutually exclusive with irreversibleStartBlockNum

	logger *zap.Logger
}

type JoiningSourceOption = func(s *JoiningSource)

//func NewJoiningSourceFromCursor(leftSourceFactory, rightSourceFactory ForkableSourceFactory, h Handler, logger *zap.Logger, cursor *Cursor, options ...JoiningSourceOption) *JoiningSource {

func NewJoiningSource(leftSourceFactory, rightSourceFactory ForkableSourceFactory, h Handler, logger *zap.Logger, options ...JoiningSourceOption) *JoiningSource {
	s := &JoiningSource{
		leftSourceFactory:  leftSourceFactory,
		rightSourceFactory: rightSourceFactory,
		handler:            h,
		logger:             logger,
	}

	for _, option := range options {
		option(s)
	}

	s.logger.Info("creating new joining source")
	s.Shutter = shutter.New()
	s.Shutter.OnTerminating(func(err error) {
		s.sourcesLock.Lock()
		defer s.sourcesLock.Unlock()

		if s.leftSource != nil {
			s.logger.Debug("shutting down left-side source")
			s.leftSource.Shutdown(err)
		}
		if s.rightSource != nil {
			s.logger.Debug("shutting down right-side source")
			s.rightSource.Shutdown(err)
		}
	})

	return s
}

func (s *JoiningSource) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

func (s *JoiningSource) Run() {
	s.Shutdown(s.run())
}

func (s *JoiningSource) run() error {
	return nil
}

//func (s *JoiningSource) run() error {
//	s.logger.Info("joining Source is now running")
//	s.sourcesLock.Lock()
//
//	// ? am I cursor-bound or startBlock-bound ?
//	//if s.rightSourceFactory()
//
//	if s.fileSourceFactory != nil {
//		s.fileSource = s.fileSourceFactory(HandlerFunc(s.incomingFromFile))
//		s.fileSource.SetLogger(s.logger.Named("file"))
//	}
//
//	if s.liveSourceFactory != nil {
//		liveFactory := s.liveSourceFactory
//		s.liveSource = liveFactory(HandlerFunc(s.incomingFromLive))
//		s.liveSource.SetLogger(s.logger.Named("live"))
//	}
//
//	_ = s.LockedInit(func() error {
//		if s.fileSource != nil {
//			s.fileSource.OnTerminating(func(err error) {
//				if err := s.fileSource.Err(); err != nil {
//					s.Shutdown(fmt.Errorf("file source failed: %w", err))
//				}
//				if !s.livePassThru {
//					s.Shutdown(fmt.Errorf("file source was shut down and we're not live"))
//				}
//			})
//
//			go s.fileSource.Run()
//		}
//
//		if s.liveSource != nil {
//			s.liveSource.OnTerminating(func(err error) {
//				if err != nil {
//					s.Shutdown(fmt.Errorf("live source failed: %w", err))
//				} else {
//					s.Shutdown(nil)
//				}
//			})
//
//			if s.startLiveImmediately == nil {
//				targetBlockIDSet := s.targetBlockID != "" // allows quick reconnection from eternal source: default behavior
//				s.startLiveImmediately = &targetBlockIDSet
//			}
//
//			go func() {
//				for s.tracker != nil && !*s.startLiveImmediately {
//					if s.IsTerminating() { // no more need to start live if joiningSource is shut down
//						return
//					}
//
//					ctx, cancel := context.WithTimeout(context.Background(), s.trackerTimeout)
//					fileBlock, liveBlock, near, err := s.tracker.IsNearWithResults(ctx, FileSourceHeadTarget, LiveSourceHeadTarget)
//					if err == nil && near {
//						zlog.Debug("tracker near, starting live source")
//						cancel()
//						break
//					}
//
//					// manually checking nearness to targetBlockNum if not zero
//					if liveBlock != nil {
//						s.handlerLock.Lock()
//						s.state.lastLiveBlock = liveBlock.Num()
//						s.handlerLock.Unlock()
//						if s.targetBlockNum != 0 && s.tracker.IsNearManualCheck(s.targetBlockNum, liveBlock.Num()) {
//							s.logger.Debug("tracker near 'targetBlockNum', starting live source")
//							cancel()
//							break
//						}
//					}
//
//					zlog.Debug("tracker returned not ready", zap.Error(err))
//					if fileBlock == nil || fileBlock.Num() == 0 {
//						time.Sleep(200 * time.Millisecond)
//						cancel()
//					}
//					<-ctx.Done()
//					cancel()
//					continue
//				}
//				s.logger.Debug("calling run on live source")
//				s.liveSource.Run()
//			}()
//		}
//		return nil
//	})
//
//	s.sourcesLock.Unlock()
//
//	<-s.Terminating()
//
//	return s.Err()
//}
//
//func (s *JoiningSource) incomingFromFile(blk *Block, obj interface{}) error {
//	s.handlerLock.Lock()
//	defer s.handlerLock.Unlock()
//
//	if s.IsTerminating() {
//		return fmt.Errorf("not processing blocks when down")
//	}
//
//	if s.livePassThru {
//		return fmt.Errorf("fileSource should be shut down, incomingFromFile should not be called")
//	}
//
//	s.state.lastFileBlock = blk.Num()
//	if s.liveBuffer.Exists(blk.ID()) {
//		s.livePassThru = true
//		err := s.processLiveBuffer(blk)
//		if err != nil {
//			return err
//		}
//
//		s.logger.Info("shutting file source, switching to live (from a file block matching)")
//
//		s.fileSource.Shutdown(nil)
//		s.liveBuffer = nil
//		return nil
//	}
//	if blk.Num() > s.highestFileProcessedBlockNum {
//		s.highestFileProcessedBlockNum = blk.Num()
//	}
//	s.lastFileProcessedBlock = blk
//	s.logger.Debug("processing from file", zap.Stringer("block_num", blk))
//	return s.handler.ProcessBlock(blk, obj)
//
//}
//
//func (s *JoiningSource) incomingFromLive(blk *Block, obj interface{}) error {
//	s.handlerLock.Lock()
//	defer s.handlerLock.Unlock()
//
//	if s.IsTerminating() {
//		return fmt.Errorf("not processing blocks when down")
//	}
//
//	s.logger.Info("incoming live block", zap.Stringer("block", blk), zap.Bool("live_pass_through", s.livePassThru))
//	s.state.lastLiveBlock = blk.Num()
//
//	if s.livePassThru {
//		if tracer.Enabled() {
//			s.logger.Debug("processing from live", zap.Stringer("block", blk))
//		} else if blk.Number%600 == 0 {
//			s.logger.Debug("processing from live (1/600 sampling)", zap.Stringer("block", blk))
//		}
//
//		return s.handler.ProcessBlock(blk, obj)
//	}
//
//	if s.lastFileProcessedBlock.ID() == blk.ID() {
//		s.livePassThru = true
//		s.logger.Info("shutting file source, switching to live (from a live block matching)", zap.Stringer("block", blk))
//		s.fileSource.Shutdown(nil)
//		s.liveBuffer = nil
//		return nil
//	}
//
//	if s.targetBlockNum != 0 && blk.Num() == s.targetBlockNum && s.lastFileProcessedBlock == nil {
//		s.livePassThru = true
//		s.logger.Info("shutting file source, starting from live at requested block ID", zap.Stringer("block", blk))
//		s.fileSource.Shutdown(nil)
//		s.liveBuffer = nil
//		return s.handler.ProcessBlock(blk, obj)
//	}
//
//	if s.targetBlockID != "" && blk.ID() == s.targetBlockID && s.lastFileProcessedBlock == nil {
//		s.livePassThru = true
//		s.logger.Info("shutting file source, starting from live at requested block ID", zap.Stringer("block", blk))
//		s.fileSource.Shutdown(nil)
//		s.liveBuffer = nil
//		return s.handler.ProcessBlock(blk, obj)
//	}
//
//	if s.liveBuffer.Len() >= s.liveBufferSize {
//		s.liveBuffer.Delete(s.liveBuffer.Tail())
//	}
//	s.liveBuffer.AppendHead(&PreprocessedBlock{Block: blk, Obj: obj})
//	s.logger.Info("added live block to buffer", zap.Stringer("block", blk), zap.Int("buffer_size", s.liveBuffer.Len()))
//	return nil
//}
//
//func (s *JoiningSource) LastFileBlockRefGetter(_ context.Context) (BlockRef, error) {
//	// TODO: lock if needed
//	if s.lastFileProcessedBlock != nil {
//		return s.lastFileProcessedBlock, nil
//	}
//	return nil, ErrTrackerBlockNotFound
//}
//
//func (s *JoiningSource) processLiveBuffer(liveBlock *Block) (err error) {
//	liveID := liveBlock.ID()
//	s.logger.Debug("looking for ID", zap.String("live_id", liveID), zap.Uint64("live_num", liveBlock.Num()))
//	gatePassed := false
//	count := 0
//	for _, blk := range s.liveBuffer.AllBlocks() {
//		blk, ok := blk.(*PreprocessedBlock)
//		if !ok {
//			panic("buffer contained non-block object")
//		}
//
//		if blk.ID() == liveID {
//			gatePassed = true
//		}
//
//		if gatePassed {
//			count += 1
//			s.logger.Debug("processing from live buffer", zap.Stringer("block", blk))
//			if err = s.handler.ProcessBlock(blk.Block, blk.Obj); err != nil {
//				return err
//			}
//		}
//	}
//	s.logger.Debug("finished processing liveBuffer", zap.Bool("gatePassed", gatePassed), zap.Int("count", count), zap.Int("len_livebuffer", len(s.liveBuffer.AllBlocks())), zap.Bool("exists_in_livebuffer", s.liveBuffer.Exists(liveID)))
//	return nil
//}
//
////type joinSourceState struct {
////	lastFileBlock uint64
////	lastLiveBlock uint64
////	logger        *zap.Logger
////	seenLive      bool
////}
//
////func (s *joinSourceState) logd(joiningSource *JoiningSource) {
////	go func() {
////		sleepDuration := 2 * time.Second
////		time.Sleep(sleepDuration) // start logging after we've had a chance to connect
////		for {
////			if joiningSource.IsTerminating() {
////				return
////			}
////			sleepDuration = s.log(joiningSource)
////			time.Sleep(sleepDuration)
////		}
////	}()
////}
//
//// TODO rewrite thise
////func (s *joinSourceState) log(joiningSource *JoiningSource) time.Duration {
////	if joiningSource.livePassThru {
////		if s.seenLive {
////			s.logger.Debug("joining state LIVE", zap.Uint64("last_live_block", s.lastLiveBlock))
////		} else {
////			s.logger.Info("joining state LIVE", zap.Uint64("last_live_block", s.lastLiveBlock))
////			s.seenLive = true
////		}
////		return 30 * time.Second
////	} else {
////		var tailNum, headNum uint64
////		if tail := joiningSource.liveBuffer.Tail(); tail != nil {
////			tailNum = tail.Num()
////		}
////		if head := joiningSource.liveBuffer.Head(); head != nil {
////			headNum = head.Num()
////		}
////		s.logger.Info("joining state JOINING",
////			zap.Int64("block_behind_live", int64(s.lastLiveBlock)-int64(s.lastFileBlock)),
////			zap.Uint64("last_file_block", s.lastFileBlock),
////			zap.Uint64("last_live_block", s.lastLiveBlock),
////			zap.Uint64("buffer_lower_block", tailNum),
////			zap.Uint64("buffer_higher_block", headNum),
////		)
////		return 5 * time.Second
////	}
////}
