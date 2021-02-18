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

package forkable

import (
	"context"
	"fmt"
	"time"

	"github.com/dfuse-io/bstream"
	pbblockmeta "github.com/dfuse-io/pbgo/dfuse/blockmeta/v1"
	"go.uber.org/zap"
)

type Forkable struct {
	logger        *zap.Logger
	handler       bstream.Handler
	forkDB        *ForkDB
	lastBlockSent *bstream.Block
	lastLIBSent   bstream.BlockRef
	filterSteps   StepType

	ensureBlockFlows  bstream.BlockRef
	ensureBlockFlowed bool
	gateCursor        *Cursor

	ensureAllBlocksTriggerLongestChain bool

	includeInitialLIB bool

	irrChecker                      *irreversibilityChecker
	lastLIBNumFromStartOrIrrChecker uint64

	lastLongestChain []*Block
}

type irreversibilityChecker struct {
	answer             chan bstream.BasicBlockRef
	blockIDClient      pbblockmeta.BlockIDClient
	delayBetweenChecks time.Duration
	lastCheckTime      time.Time
}

func (ic *irreversibilityChecker) CheckAsync(blk bstream.BlockRef, libNum uint64) {
	if blk.Num() < libNum+bstream.GetMaxNormalLIBDistance { // only kick in when lib gets too far
		return
	}
	if time.Since(ic.lastCheckTime) < ic.delayBetweenChecks {
		return
	}
	ic.lastCheckTime = time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), ic.delayBetweenChecks)
		defer cancel()

		resp, err := ic.blockIDClient.NumToID(ctx, &pbblockmeta.NumToIDRequest{
			BlockNum: blk.Num(),
		})
		if err != nil {
			zlog.Warn("forkable cannot fetch BlockIDServer (blockmeta) to resolve block ID", zap.Error(err), zap.Uint64("block_num", blk.Num()))
			return
		}
		if resp.Irreversible && resp.Id == blk.ID() {
			ic.answer <- bstream.NewBlockRef(blk.ID(), blk.Num())
		}
	}()
}

func (ic *irreversibilityChecker) Found() (out bstream.BasicBlockRef, found bool) {
	select {
	case out = <-ic.answer:
		found = true
		return
	default:
	}
	return out, false
}

type ForkableObject struct {
	Step StepType

	HandoffCount int

	// The three following fields are filled when handling multi-block steps, like when passing Irreversibile segments, the whole segment is represented in here.
	StepCount  int                          // Total number of steps in multi-block steps.
	StepIndex  int                          // Index for the current block
	StepBlocks []*bstream.PreprocessedBlock // You can decide to process them when StepCount == StepIndex +1 or when StepIndex == 0 only.

	ForkDB *ForkDB // ForkDB is a reference to the `Forkable`'s ForkDB instance. Provided you don't use it in goroutines, it is safe for use in `ProcessBlock` calls.

	headBlock   bstream.BlockRef
	block       bstream.BlockRef
	lastLIBSent bstream.BlockRef

	// Object that was returned by PreprocessBlock(). Could be nil
	Obj interface{}
}

func (fobj *ForkableObject) Cursor() *Cursor {
	if fobj == nil || fobj.block == nil || fobj.headBlock == nil || fobj.lastLIBSent == nil {
		return EmptyCursor
	}

	step := fobj.Step
	if step == StepRedo {
		step = StepNew
	}

	lib := fobj.lastLIBSent
	if bstream.EqualsBlockRefs(bstream.BlockRefEmpty, lib) && fobj.ForkDB.HasLIB() {
		lib = bstream.NewBlockRef(fobj.ForkDB.libID, fobj.ForkDB.libNum)
	}

	return &Cursor{
		Step:      step,
		Block:     fobj.block,
		HeadBlock: fobj.headBlock,
		LIB:       lib,
	}
}

type ForkableBlock struct {
	Block     *bstream.Block
	Obj       interface{}
	SentAsNew bool
}

func New(h bstream.Handler, opts ...Option) *Forkable {
	f := &Forkable{
		filterSteps:      StepsAll,
		handler:          h,
		forkDB:           NewForkDB(),
		ensureBlockFlows: bstream.BlockRefEmpty,
		lastLIBSent:      bstream.BlockRefEmpty,
		logger:           zlog,
	}

	for _, opt := range opts {
		opt(f)
	}

	// Done afterwards so forkdb can get configured forkable logger from options
	f.forkDB.logger = f.logger

	return f
}

// NewWithLIB DEPRECATED, use `New(h, WithExclusiveLIB(libID))`.  Also use `EnsureBlockFlows`, `WithFilters`, `EnsureAllBlocksTriggerLongestChain` options
func NewWithLIB(libID bstream.BlockRef, h bstream.Handler) { return }

func (p *Forkable) targetChainBlock(blk *bstream.Block) bstream.BlockRef {
	if p.ensureBlockFlows.ID() != "" && !p.ensureBlockFlowed {
		return p.ensureBlockFlows
	}

	return blk
}

func (p *Forkable) matchFilter(filter StepType) bool {
	return p.filterSteps&filter != 0
}

func (p *Forkable) computeNewLongestChain(ppBlk *ForkableBlock) []*Block {
	longestChain := p.lastLongestChain
	blk := ppBlk.Block

	canSkipRecompute := false
	if len(longestChain) != 0 &&
		blk.PreviousID() == longestChain[len(longestChain)-1].BlockID && // optimize if adding block linearly
		p.forkDB.LIBNum()+1 == longestChain[0].BlockNum { // do not optimize if the lib moved (should truncate up to lib)
		canSkipRecompute = true
	}

	if canSkipRecompute {
		longestChain = append(longestChain, &Block{
			BlockID:  blk.ID(), // NOTE: we don't want "Previous" because ReversibleSegment does not give them
			BlockNum: blk.Num(),
			Object:   ppBlk,
		})
	} else {
		longestChain = p.forkDB.ReversibleSegment(p.targetChainBlock(blk))
	}
	p.lastLongestChain = longestChain
	return longestChain

}

func (p *Forkable) feedCursorStateRestorer(blk *bstream.Block, obj interface{}) (err error) {

	ppBlk := &ForkableBlock{Block: blk, Obj: obj}
	previousRef := bstream.NewBlockRef(blk.PreviousID(), blk.Num()-1)
	p.forkDB.AddLink(blk.AsRef(), previousRef, ppBlk)

	// FIXME: eventually check if all of those are linked in a full segment ?
	cur := p.gateCursor
	if !p.forkDB.Exists(cur.HeadBlock.ID()) ||
		!p.forkDB.Exists(cur.Block.ID()) ||
		!p.forkDB.Exists(cur.LIB.ID()) {
		if traceEnabled {
			zlog.Debug("missing at least one block", zap.Stringer("cursor", cur))
		}
		return
	}

	p.gateCursor = nil
	p.forkDB.InitLIB(cur.LIB)
	p.lastLIBSent = cur.LIB

	switch cur.Step {
	case StepNew, StepUndo:
		var headBlock *ForkableBlock
		for _, fobj := range p.forkDB.objects {
			fblk := fobj.(*ForkableBlock)
			if fblk.Block.Number < cur.Block.Num() {
				fblk.SentAsNew = true // so they dont get sent again
			}
			if fblk.Block.ID() == cur.HeadBlock.ID() {
				headBlock = fblk // we need this one
			}
			if fblk.Block.ID() == cur.Block.ID() {
				fblk.SentAsNew = true
				if cur.Step == StepNew {
					p.lastBlockSent = fblk.Block
				} else { // UNDO
					p.lastBlockSent = p.forkDB.objects[fblk.Block.PreviousID()].(*ForkableBlock).Block
				}
			}
		}
		// we want the head block to 'come in as new'
		if cur.HeadBlock.ID() != cur.Block.ID() {
			p.forkDB.DeleteLink(cur.HeadBlock.ID())
			return p.ProcessBlock(headBlock.Block, headBlock.Obj)
		}
		return
	case StepIrreversible:
		var headBlock *ForkableBlock
		for _, fobj := range p.forkDB.objects {
			fblk := fobj.(*ForkableBlock)
			if fblk.Block.Number < cur.HeadBlock.Num() {
				fblk.SentAsNew = true
			}
			if fblk.Block.ID() == cur.HeadBlock.ID() {
				fblk.SentAsNew = true
				headBlock = fblk
			}
		}
		libRef := p.forkDB.BlockInCurrentChain(headBlock.Block, headBlock.Block.LibNum)
		hasNew, irreversibleSegment, _ := p.forkDB.HasNewIrreversibleSegment(libRef)
		if hasNew {
			_ = p.forkDB.MoveLIB(libRef)
			if err := p.processIrreversibleSegment(irreversibleSegment, headBlock.Block); err != nil {
				return err
			}
		}
		p.lastBlockSent = headBlock.Block
		return
	default:
		return fmt.Errorf("unsupported cursor step %q", cur.Step)
	}
}

func (p *Forkable) ProcessBlock(blk *bstream.Block, obj interface{}) error {
	if p.gateCursor != nil {
		return p.feedCursorStateRestorer(blk, obj)
	}

	if blk.Num() < p.forkDB.LIBNum() && p.lastBlockSent != nil {
		return nil
	}

	zlogBlk := p.logger.With(zap.Stringer("block", blk))

	// TODO: consider an `initialHeadBlockID`, triggerNewLongestChain also when the initialHeadBlockID's BlockNum == blk.Num()
	triggersNewLongestChain := p.triggersNewLongestChain(blk)

	if traceEnabled {
		zlogBlk.Debug("processing block", zap.Bool("new_longest_chain", triggersNewLongestChain))
	} else if blk.Number%600 == 0 {
		zlogBlk.Debug("processing block (1/600 sampling)", zap.Bool("new_longest_chain", triggersNewLongestChain))
	}

	ppBlk := &ForkableBlock{Block: blk, Obj: obj}

	if p.includeInitialLIB && p.lastBlockSent == nil && blk.ID() == p.forkDB.LIBID() {
		return p.processInitialInclusiveIrreversibleBlock(blk, obj)
	}
	// special case to send the LIB if we receive it on an initlib'ed empty forkdb. Easier in some contexts.
	// ex: I have block 00000004a in my hands, and I know it is irreversible.
	//     I initLIB with 00000003a, then I send the block 00000003a in, I want it pushed :D
	// if p.lastBlockSent == nil && blk.ID() == p.forkDB.LIBID() {
	// 	zlogBlk.Debug("sending block through, it is our lib", zap.String("blk_id", blk.ID()), zap.Uint64("blk_num", blk.Num()))
	// 	return p.handler.ProcessBlock(blk, &ForkableObject{
	// 		Step:   StepNew,
	// 		ForkDB: p.forkDB,
	// 		Obj:    obj,
	// 	})
	// }

	var undos, redos []*ForkableBlock
	if p.matchFilter(StepUndo | StepRedo) {
		if triggersNewLongestChain && p.lastBlockSent != nil {
			undos, redos = p.sentChainSwitchSegments(zlogBlk, p.lastBlockSent.ID(), blk.PreviousID())
		}
	}

	previousRef := bstream.NewBlockRef(blk.PreviousID(), blk.Num()-1)
	if exists := p.forkDB.AddLink(blk, previousRef, ppBlk); exists {
		return nil
	}

	if !p.forkDB.HasLIB() { // always skip processing until LIB is set
		p.forkDB.TrySetLIB(blk, previousRef, blk.LIBNum())
	}

	if !p.forkDB.HasLIB() {
		return nil
	}

	// All this code isn't reachable unless a LIB is set in the ForkDB

	if p.irrChecker != nil && p.lastLIBNumFromStartOrIrrChecker == 0 {
		p.lastLIBNumFromStartOrIrrChecker = p.forkDB.LIBNum()
	}

	longestChain := p.computeNewLongestChain(ppBlk)
	if !triggersNewLongestChain || len(longestChain) == 0 {
		return nil
	}

	if traceEnabled {
		zlogBlk.Debug("got longest chain", zap.Int("chain_length", len(longestChain)), zap.Int("undos_length", len(undos)), zap.Int("redos_length", len(redos)))
	} else if blk.Number%600 == 0 {
		zlogBlk.Debug("got longest chain (1/600 sampling)", zap.Int("chain_length", len(longestChain)), zap.Int("undos_length", len(undos)), zap.Int("redos_length", len(redos)))
	}

	if p.matchFilter(StepUndo) {
		if err := p.processBlocks(blk, undos, StepUndo); err != nil {
			return err
		}
	}

	if p.matchFilter(StepRedo) {
		if err := p.processBlocks(blk, redos, StepRedo); err != nil {
			return err
		}
	}

	if err := p.processNewBlocks(longestChain); err != nil {
		return err
	}

	if p.lastBlockSent == nil {
		return nil
	}

	newLIBNum := p.lastBlockSent.LIBNum()
	newHeadBlock := p.lastBlockSent

	if newLIBNum < p.lastLIBNumFromStartOrIrrChecker {
		// we've been truncated before
		newLIBNum = p.lastLIBNumFromStartOrIrrChecker
	}

	libRef := p.forkDB.BlockInCurrentChain(newHeadBlock, newLIBNum)
	if libRef.ID() == "" {

		// this happens when the lib was set initially and we have not yet filled the lib->head buffer
		if traceEnabled {
			zlogBlk.Debug("missing links to reach lib_num", zap.Stringer("new_head_block", newHeadBlock), zap.Uint64("new_lib_num", newLIBNum))
		} else if newHeadBlock.Number%600 == 0 {
			zlogBlk.Debug("missing links to reach lib_num (1/600 sampling)", zap.Stringer("new_head_block", newHeadBlock), zap.Uint64("new_lib_num", newLIBNum))
		}

		return nil
	}

	if p.irrChecker != nil {
		p.irrChecker.CheckAsync(p.lastBlockSent, newLIBNum)
		if newLIB, found := p.irrChecker.Found(); found {
			if newLIB.Num() > libRef.Num() && newLIB.Num() < newHeadBlock.Num() {
				zlog.Info("irreversibilityChecker moving LIB from blockmeta reference because it is not advancing in chain", zap.Stringer("new_lib", newLIB), zap.Uint64("dposLIBNum", blk.LIBNum()))
				libRef = newLIB
				p.lastLIBNumFromStartOrIrrChecker = newLIB.Num()
			}
		}
	}

	// TODO: check preconditions here, and decide on whether we
	// continue or not early return would be perfect if there's no
	// `irreversibleSegment` or `stalledBlocks` to process.
	hasNew, irreversibleSegment, stalledBlocks := p.forkDB.HasNewIrreversibleSegment(libRef)
	if !hasNew {
		return nil
	}

	if traceEnabled {
		zlogBlk.Debug("moving lib", zap.Stringer("lib", libRef))
	} else if libRef.Num()%600 == 0 {
		zlogBlk.Debug("moving lib (1/600)", zap.Stringer("lib", libRef))
	}

	_ = p.forkDB.MoveLIB(libRef)

	if err := p.processIrreversibleSegment(irreversibleSegment, ppBlk.Block); err != nil {
		return err
	}

	if err := p.processStalledSegment(stalledBlocks, ppBlk.Block); err != nil {
		return err
	}

	return nil
}

func ids(blocks []*ForkableBlock) (ids []string) {
	ids = make([]string, len(blocks))
	for i, obj := range blocks {
		ids[i] = obj.Block.ID()
	}

	return
}

func (p *Forkable) sentChainSwitchSegments(zlogger *zap.Logger, currentHeadBlockID string, newHeadsPreviousID string) (undos []*ForkableBlock, redos []*ForkableBlock) {
	if currentHeadBlockID == newHeadsPreviousID {
		return
	}

	undoIDs, redoIDs := p.forkDB.ChainSwitchSegments(currentHeadBlockID, newHeadsPreviousID)

	undos = p.sentChainSegment(undoIDs, false)
	redos = p.sentChainSegment(redoIDs, true)
	return
}

func (p *Forkable) sentChainSegment(ids []string, doingRedos bool) (ppBlocks []*ForkableBlock) {
	for _, blockID := range ids {
		blkObj := p.forkDB.BlockForID(blockID)
		if blkObj == nil {
			panic(fmt.Errorf("block for id returned nil for id %q, this would panic later on", blockID))
		}

		ppBlock := blkObj.Object.(*ForkableBlock)
		if doingRedos && !ppBlock.SentAsNew {
			continue
		}

		ppBlocks = append(ppBlocks, ppBlock)
	}
	return
}

func (p *Forkable) processBlocks(currentBlock bstream.BlockRef, blocks []*ForkableBlock, step StepType) error {
	var objs []*bstream.PreprocessedBlock

	for _, block := range blocks {
		objs = append(objs, &bstream.PreprocessedBlock{
			Block: block.Block,
			Obj:   block.Obj,
		})
	}

	for idx, block := range blocks {

		fo := &ForkableObject{
			Step:        step,
			ForkDB:      p.forkDB,
			lastLIBSent: p.lastLIBSent,
			Obj:         block.Obj,
			headBlock:   currentBlock,
			block:       block.Block,

			StepIndex:  idx,
			StepCount:  len(blocks),
			StepBlocks: objs,
		}

		err := p.handler.ProcessBlock(block.Block, fo)

		p.logger.Debug("sent block", zap.Stringer("block", block.Block), zap.Stringer("step_type", step))
		if err != nil {
			return fmt.Errorf("process block [%s] step=%q: %w", block.Block, step, err)
		}
	}
	return nil
}

func (p *Forkable) processNewBlocks(longestChain []*Block) (err error) {
	headBlock := longestChain[len(longestChain)-1]
	for _, b := range longestChain {
		ppBlk := b.Object.(*ForkableBlock)
		if ppBlk.SentAsNew {
			// Sadly, there was a debug log line here, but it's so a pain to have when debug, since longuest
			// chain is iterated over and over again generating tons of this (now gone) log line. For this,
			// it was removed to make it easier to track what happen.
			continue
		}

		if p.matchFilter(StepNew) {

			fo := &ForkableObject{
				headBlock:   headBlock.AsRef(),
				block:       b.AsRef(),
				Step:        StepNew,
				ForkDB:      p.forkDB,
				lastLIBSent: p.lastLIBSent,
				Obj:         ppBlk.Obj,
			}

			err = p.handler.ProcessBlock(ppBlk.Block, fo)
			if err != nil {
				return
			}
		}

		if traceEnabled {
			p.logger.Debug("sending block as new to consumer", zap.Stringer("block", ppBlk.Block))
		} else if ppBlk.Block.Number%600 == 0 {
			p.logger.Debug("sending block as new to consumer (1/600 sampling)", zap.Stringer("block", ppBlk.Block))
		}

		zlog.Debug("block sent as new", zap.Stringer("pblk.block", ppBlk.Block))
		p.blockFlowed(ppBlk.Block)
		ppBlk.SentAsNew = true
		p.lastBlockSent = ppBlk.Block
	}

	return
}

func (p *Forkable) processInitialInclusiveIrreversibleBlock(blk *bstream.Block, obj interface{}) error {
	// Normally extracted from ForkDB, we create it here:
	singleBlock := &Block{
		BlockID:  blk.ID(),
		BlockNum: blk.Num(),
		// Other fields not needed by `processNewBlocks`
		Object: &ForkableBlock{
			// WARN: this ForkDB doesn't have a reference to the current block, hopefully downstream doesn't need that (!)
			Block: blk,
			Obj:   obj,
		},
	}

	tinyChain := []*Block{singleBlock}

	if err := p.processNewBlocks(tinyChain); err != nil {
		return err
	}

	if err := p.processIrreversibleSegment(tinyChain, blk); err != nil {
		return err
	}

	return nil
}

func (p *Forkable) processIrreversibleSegment(irreversibleSegment []*Block, headBlock bstream.BlockRef) error {
	if p.matchFilter(StepIrreversible) {
		var irrGroup []*bstream.PreprocessedBlock
		for _, irrBlock := range irreversibleSegment {
			preprocBlock := irrBlock.Object.(*ForkableBlock)
			irrGroup = append(irrGroup, &bstream.PreprocessedBlock{
				Block: preprocBlock.Block,
				Obj:   preprocBlock.Obj,
			})
		}

		for idx, irrBlock := range irreversibleSegment {
			preprocBlock := irrBlock.Object.(*ForkableBlock)

			objWrap := &ForkableObject{
				Step:        StepIrreversible,
				ForkDB:      p.forkDB,
				lastLIBSent: preprocBlock.Block.AsRef(), // we are that lastLIBSent
				Obj:         preprocBlock.Obj,
				block:       preprocBlock.Block.AsRef(),
				headBlock:   headBlock,

				StepIndex:  idx,
				StepCount:  len(irreversibleSegment),
				StepBlocks: irrGroup,
			}

			if err := p.handler.ProcessBlock(preprocBlock.Block, objWrap); err != nil {
				return err
			}
			p.lastLIBSent = irrBlock.AsRef()
		}
	}
	return nil
}

func (p *Forkable) processStalledSegment(stalledBlocks []*Block, headBlock bstream.BlockRef) error {
	if p.matchFilter(StepStalled) {
		var stalledGroup []*bstream.PreprocessedBlock
		for _, staleBlock := range stalledBlocks {
			preprocBlock := staleBlock.Object.(*ForkableBlock)
			stalledGroup = append(stalledGroup, &bstream.PreprocessedBlock{
				Block: preprocBlock.Block,
				Obj:   preprocBlock.Obj,
			})
		}

		for idx, staleBlock := range stalledBlocks {
			preprocBlock := staleBlock.Object.(*ForkableBlock)

			objWrap := &ForkableObject{
				Step:        StepStalled,
				ForkDB:      p.forkDB,
				lastLIBSent: p.lastLIBSent,
				Obj:         preprocBlock.Obj,
				block:       staleBlock.AsRef(),
				headBlock:   headBlock,

				StepIndex:  idx,
				StepCount:  len(stalledBlocks),
				StepBlocks: stalledGroup,
			}

			if err := p.handler.ProcessBlock(preprocBlock.Block, objWrap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Forkable) blockFlowed(blockRef bstream.BlockRef) {
	if p.ensureBlockFlows.ID() == "" {
		return
	}

	if p.ensureBlockFlowed {
		return
	}

	if blockRef.ID() == p.ensureBlockFlows.ID() {
		p.ensureBlockFlowed = true
	}
}

func (p *Forkable) triggersNewLongestChain(blk *bstream.Block) bool {
	if p.ensureAllBlocksTriggerLongestChain {
		return true
	}

	if p.lastBlockSent == nil {
		return true
	}

	if blk.Num() > p.lastBlockSent.Num() {
		return true
	}

	return false
}
