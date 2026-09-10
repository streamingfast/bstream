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

package hub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streamingfast/dstore"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/forkable"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// ForkableHub gives you block Sources for blocks close to head
// it keeps reversible segment in a Forkable
// it keeps small final segment in a buffer
type ForkableHub struct {
	*shutter.Shutter

	logger *zap.Logger

	forkable *forkable.Forkable

	keepFinalBlocks int

	subscribers       []*Subscription
	sourceChannelSize int

	liveSourceFactory bstream.SourceFactory
	oneBlocksStore    dstore.Store

	Ready chan struct{}

	maxConsecutiveUnlinkableBlocks int // 0 means disabled
	consecutiveUnlinkableBlocks    int

	oneBlockDownloadConcurrency int
}

const defaultOneBlockDownloadConcurrency = 32

func NewForkableHub(liveSourceFactory bstream.SourceFactory, keepFinalBlocks int, oneBlocksStore dstore.Store, extraForkableOptions ...forkable.Option) *ForkableHub {
	return newForkableHub(liveSourceFactory, keepFinalBlocks, oneBlocksStore, nil, extraForkableOptions...)
}

// NewForkableHubWithOptions is like NewForkableHub but also accepts hub-level
// options such as WithMaxConsecutiveUnlinkableBlocks.
func NewForkableHubWithOptions(liveSourceFactory bstream.SourceFactory, keepFinalBlocks int, oneBlocksStore dstore.Store, hubOptions []Option, extraForkableOptions ...forkable.Option) *ForkableHub {
	return newForkableHub(liveSourceFactory, keepFinalBlocks, oneBlocksStore, hubOptions, extraForkableOptions...)
}

func newForkableHub(liveSourceFactory bstream.SourceFactory, keepFinalBlocks int, oneBlocksStore dstore.Store, hubOptions []Option, extraForkableOptions ...forkable.Option) *ForkableHub {
	sourceChanSize := 100
	if os.Getenv("SOURCE_CHAN_SIZE") != "" {
		newSize, err := strconv.Atoi(os.Getenv("SOURCE_CHAN_SIZE"))
		if err != nil {
			zlog.Warn("invalid SOURCE_CHAN_SIZE, ignoring", zap.Error(err))
		}
		sourceChanSize = newSize
	}

	hub := &ForkableHub{
		Shutter:           shutter.New(),
		logger:            zlog,
		liveSourceFactory: liveSourceFactory,
		keepFinalBlocks:   keepFinalBlocks,
		sourceChannelSize: sourceChanSize, // number of blocks that can add up before the subscriber processes them
		oneBlocksStore:    oneBlocksStore,
		Ready:             make(chan struct{}),

		oneBlockDownloadConcurrency: defaultOneBlockDownloadConcurrency,
	}

	// Apply hub-level options first so that a customized logger (via WithLogger)
	// is propagated to the inner forkable created below.
	for _, opt := range hubOptions {
		opt(hub)
	}

	forkableOptions := append([]forkable.Option{
		forkable.WithLogger(hub.logger),
		forkable.HoldBlocksUntilLIB(),
		forkable.WithKeptFinalBlocks(keepFinalBlocks),
		forkable.WithFilters(bstream.StepsAllWithPartial),
	}, extraForkableOptions...)

	hub.forkable = forkable.New(bstream.HandlerFunc(hub.broadcastBlock), forkableOptions...)

	hub.logger.Info("New forkable hub initialized",
		zap.Int("source_chan_size", sourceChanSize),
		zap.Int("keep_final_blocks", keepFinalBlocks),
	)

	hub.OnTerminating(func(err error) {
		for _, sub := range hub.subscribers {
			sub.Shutdown(err)
		}
	})

	return hub
}

// Option configures a ForkableHub.
type Option func(h *ForkableHub)

// WithMaxConsecutiveUnlinkableBlocks instructs the hub to shut itself down
// (with errRestartRequired) if it has already passed readiness and it receives
// count consecutive blocks that cannot be linked to its current head, even
// after attempting to fill the gap from the one-block store.
// A count of 0 (the default) disables the check.
func WithMaxConsecutiveUnlinkableBlocks(count int) Option {
	return func(h *ForkableHub) {
		h.maxConsecutiveUnlinkableBlocks = count
	}
}

// WithLogger sets the logger used by the hub and, by default, propagated to its
// inner forkable. When unset, the package-level "bstream" logger is used, which
// makes every hub instance log under the same identifier. Callers should pass
// their component logger (e.g. relayer, firehose, tier1) so log lines such as
// "processing block" are attributed to the right component.
func WithLogger(logger *zap.Logger) Option {
	return func(h *ForkableHub) {
		h.logger = logger
	}
}

// WithOneBlockDownloadConcurrency sets how many one-block files the hub downloads at once
// while bootstrapping and while filling the gap before a live block it cannot link. Blocks
// are still processed in block order. Defaults to 32; values below 1 are treated as 1.
func WithOneBlockDownloadConcurrency(count int) Option {
	return func(h *ForkableHub) {
		h.oneBlockDownloadConcurrency = max(count, 1)
	}
}

func (h *ForkableHub) LowestBlockNum() uint64 {
	if h != nil && h.IsReady() {
		return h.forkable.LowestBlockNum()
	}
	return 0
}

func (h *ForkableHub) GetBlock(num uint64, id string) (out *pbbstream.Block) {
	if id == "" {
		return h.forkable.CanonicalBlockAt(num)
	}
	for _, blk := range h.forkable.AllBlocksAt(num) {
		if id == blk.Id {
			return blk
		}
	}
	return nil
}

func (h *ForkableHub) GetBlockByHash(id string) (out *pbbstream.Block) {
	return h.forkable.GetBlockByHash(id)
}

func (h *ForkableHub) HeadInfo() (headNum uint64, headID string, headTime time.Time, libNum uint64, err error) {
	if h != nil && h.IsReady() {
		headNum, headID, headTime, libNum, err = h.forkable.HeadInfo()
		h.logger.Debug("forkable hub head info", zap.Uint64("head_num", headNum), zap.String("head_id", headID), zap.Time("head_time", headTime), zap.Uint64("lib_num", libNum))
		return
	}
	zlog.Debug("forkable hub not ready")
	err = fmt.Errorf("not ready")
	return
}

func (h *ForkableHub) HeadNum() uint64 {
	if h != nil && h.IsReady() {
		return h.forkable.HeadNum()
	}
	return 0
}
func (h *ForkableHub) MatchSuffix(req string) bool {
	ids := h.forkable.AllIDs()
	for _, id := range ids {
		if strings.HasSuffix(id, req) {
			return true
		}
	}
	return false
}

func (h *ForkableHub) IsReady() bool {
	select {
	case <-h.Ready:
		return true
	default:
		return false
	}
}

// subscribe must be called while hub is locked
func (h *ForkableHub) subscribe(handler bstream.Handler, initialBlocks []*bstream.PreprocessedBlock, withPartials bool) *Subscription {
	chanSize := h.sourceChannelSize + len(initialBlocks)
	sub := NewSubscription(handler, chanSize, withPartials)
	for _, ppblk := range initialBlocks {
		_ = sub.push(ppblk)
	}
	h.subscribers = append(h.subscribers, sub)
	return sub
}

// unsubscribe must be called while hub is locked
func (h *ForkableHub) unsubscribe(removeSub *Subscription) {
	var newSubscriber []*Subscription
	for _, sub := range h.subscribers {
		if sub != removeSub {
			newSubscriber = append(newSubscriber, sub)
		}
	}
	h.subscribers = newSubscriber
}

func (h *ForkableHub) SourceFromBlockNum(num uint64, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromNum(num, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks, true)
	}, false)
	if err != nil {
		h.logger.Debug("error getting source_from_block_num", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceFromBlockNumWithForks(num uint64, handler bstream.Handler, withPartials bool) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromNum(num, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks, withPartials)
	}, true)
	if err != nil {
		h.logger.Debug("error getting source_from_block_num", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceFromCursor(cursor *bstream.Cursor, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	err := h.forkable.CallWithBlocksFromCursor(cursor, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks, true)
	})
	if err != nil {
		h.logger.Debug("error getting source_from_cursor", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) SourceThroughCursor(startBlock uint64, cursor *bstream.Cursor, handler bstream.Handler) (out bstream.Source) {
	if h == nil {
		return nil
	}

	// cursor has already passed, ignoring it
	if cursor.Block.Num() < startBlock {
		return h.SourceFromBlockNum(startBlock, handler)
	}

	err := h.forkable.CallWithBlocksThroughCursor(startBlock, cursor, func(blocks []*bstream.PreprocessedBlock) { // Running callback func while forkable is locked
		out = h.subscribe(handler, blocks, true)
	})
	if err != nil {
		h.logger.Debug("error getting source_from_cursor", zap.Error(err))
		return nil
	}
	return
}

func (h *ForkableHub) bootstrap() error {
	ctx := context.Background()

	sortedOneBlocksFiles, err := h.WalkOneBlocksStore(ctx)
	if err != nil {
		return fmt.Errorf("walking through one blocks files: %w", err)
	}

	if len(sortedOneBlocksFiles) == 0 {
		return fmt.Errorf("no one blocks found")
	}

	mostRecentOneBlock := sortedOneBlocksFiles[len(sortedOneBlocksFiles)-1]

	_, _, _, refLibNum, _, err := bstream.ParseFilename(mostRecentOneBlock)
	if err != nil {
		return fmt.Errorf("parsing filename: %w", err)
	}
	lowestBlockNum := substractAndRoundDownBlocks(refLibNum, uint64(h.keepFinalBlocks), bstream.DefaultMergedBlocksBundleSize)

	filenames, err := h.unknownOneBlockFiles(sortedOneBlocksFiles, lowestBlockNum)
	if err != nil {
		return err
	}

	var mostRecentBlock *pbbstream.Block
	err = decodeOneBlocksInOrder(ctx, h.oneBlocksStore, filenames, h.oneBlockDownloadConcurrency, func(blk *pbbstream.Block) error {
		mostRecentBlock = blk
		if err := h.forkable.ProcessBlock(blk, nil); err != nil {
			return fmt.Errorf("processing block: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if mostRecentBlock == nil {
		return fmt.Errorf("no one blocks above libRef found")
	}

	if !h.forkable.Linkable(mostRecentBlock) {
		return fmt.Errorf("most recent one block is not linkable")
	}

	return nil
}

func (h *ForkableHub) Run() {
	liveSource := h.liveSourceFactory(h)
	liveSource.OnTerminating(h.reconnect)

	err := h.bootstrap()
	if err != nil {
		h.logger.Warn("bootstrapping from one-block-files incomplete. Will bootstrap from incoming live blocks", zap.Error(err))
	} else {
		h.logger.Info("Hub is ready")
		close(h.Ready)
	}

	liveSource.Run()

}

// isNonFinalPartial reports whether the block is a flash block that is not the last one of
// its block number, i.e. a preview of a block that has not been completed yet.
func isNonFinalPartial(blk *pbbstream.Block) bool {
	return blk.PartialIndex != 0 && !blk.LastPartial
}

func (h *ForkableHub) ProcessBlock(blk *pbbstream.Block, obj any) error {
	if !h.IsReady() && isNonFinalPartial(blk) {
		return nil // we don't get ready with partial blocks...
	}

	h.logger.Debug("processing block", zap.Uint64("block_number", blk.Number), zap.String("block_Id", blk.Id), zap.Uint64("block_lib", blk.LibNum), zap.Duration("age", time.Since(blk.Time())))

	ctx := context.Background()

	h.logger.Debug("forkable state", zap.Uint64("forkable_LibNum", h.forkable.LowestBlockNum()), zap.Uint64("forkable_headNum", h.forkable.HeadNum()))

	if h.forkable.ForkDBHasLib() && blk.Number < h.forkable.LowestBlockNum() {
		// Block is older than the current LIBNum; nothing useful to do.
		return nil
	}

	if !h.forkable.Linkable(blk) {
		if err := h.linkLiveUsingOneBlocks(ctx, blk); err != nil {
			// these would be unexpected errors, not just the case where it cannot be linked
			return err
		}
	}

	if !h.forkable.Linkable(blk) {
		// A non-final flash block is not counted. Every partial of a block fails this same
		// check for the same reason, so on a chain delivering four of them per block the
		// limit would be reached after a quarter of the blocks it names — and the check is
		// there to catch a gap the one-block store can no longer bridge, which the block's
		// final partial reports just as well. A real block and a final flash block count.
		if h.maxConsecutiveUnlinkableBlocks != 0 && !isNonFinalPartial(blk) {
			h.consecutiveUnlinkableBlocks++
			h.logger.Warn("block not linkable after one-block lookup",
				zap.Uint64("block_num", blk.Number),
				zap.Int("consecutive_unlinkable", h.consecutiveUnlinkableBlocks),
				zap.Int("max_consecutive_unlinkable", h.maxConsecutiveUnlinkableBlocks),
			)
			if h.IsReady() && h.consecutiveUnlinkableBlocks >= h.maxConsecutiveUnlinkableBlocks {
				return fmt.Errorf("received %d consecutive unlinkable blocks, %w", h.consecutiveUnlinkableBlocks, errRestartRequired)
			}
		}
	} else { // linkable
		h.consecutiveUnlinkableBlocks = 0
		if !h.IsReady() {
			h.logger.Info("Hub is ready")
			close(h.Ready)
		}
	}

	h.forkable.SetLiveMetrics()

	return h.forkable.ProcessBlock(blk, obj)
}

var errRestartRequired = errors.New("restart required")

func (h *ForkableHub) linkLiveUsingOneBlocks(ctx context.Context, blk *pbbstream.Block) error {

	lastKnownLib := h.forkable.LowestBlockNum()
	if !h.forkable.ForkDBHasLib() {
		lastKnownLib = blk.LibNum
	}

	h.logger.Debug("linking live block using one blocks", zap.Uint64("processed_block", blk.Number), zap.Uint64("last_know_lib", lastKnownLib))

	sortedOneBlocksFiles, err := h.WalkOneBlocksStoreFrom(ctx, lastKnownLib)
	if err != nil {
		return fmt.Errorf("walking through one blocks files: %w", err)
	}

	if len(sortedOneBlocksFiles) == 0 {
		h.logger.Warn("no one blocks found while trying to link live block", zap.Uint64("processed_block", blk.Number))
		return nil
	}

	filenames, err := h.unknownOneBlockFiles(sortedOneBlocksFiles, 0)
	if err != nil {
		return err
	}

	return decodeOneBlocksInOrder(ctx, h.oneBlocksStore, filenames, h.oneBlockDownloadConcurrency, func(blockFromFile *pbbstream.Block) error {
		if blockFromFile.Number == blk.LibNum && h.forkable.ForkDBHasLib() {
			if !h.forkable.Linkable(blockFromFile) {
				return fmt.Errorf("cannot link block after reconnection, %w", errRestartRequired)
			}
		}

		if err := h.forkable.ProcessBlock(blockFromFile, nil); err != nil {
			return fmt.Errorf("processing block %d: %w", blockFromFile.Number, err)
		}
		return nil
	})
}

// unknownOneBlockFiles returns, in their original order, the files at or above
// lowestBlockNum holding a block the forkable does not have yet. When several files hold
// the same block (uploaded by different readers), only the first one is kept.
func (h *ForkableHub) unknownOneBlockFiles(filenames []string, lowestBlockNum uint64) ([]string, error) {
	type blockKey struct {
		num      uint64
		suffixID string
	}
	seen := make(map[blockKey]bool)

	var out []string
	for _, filename := range filenames {
		blockNum, suffixID, _, _, _, err := bstream.ParseFilename(filename)
		if err != nil {
			return nil, fmt.Errorf("parsing filename: %w", err)
		}

		if blockNum < lowestBlockNum {
			continue
		}

		if known := h.forkable.GetBlockByHashSuffix(suffixID); known != nil && known.Number == blockNum {
			continue
		}

		key := blockKey{blockNum, suffixID}
		if seen[key] {
			continue
		}
		seen[key] = true

		out = append(out, filename)
	}
	return out, nil
}

// decodeOneBlocksInOrder downloads filenames from store with at most concurrency downloads
// in flight, and hands each decoded block to process in the order of filenames. It stops
// at the first download or process error and returns it.
func decodeOneBlocksInOrder(ctx context.Context, store dstore.Store, filenames []string, concurrency int, process func(*pbbstream.Block) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type download struct {
		filename string
		blk      *pbbstream.Block
		err      error
	}

	// Each queued entry is a download already started. The consumer waits on one more
	// download it has taken off the queue, hence the capacity of concurrency-1.
	queue := make(chan chan download, concurrency-1)
	go func() {
		defer close(queue)
		for _, filename := range filenames {
			result := make(chan download, 1)
			select {
			case queue <- result:
			case <-ctx.Done():
				return
			}
			go func() {
				blk, err := decodeOneBlockFromFilename(ctx, filename, store)
				result <- download{filename: filename, blk: blk, err: err}
			}()
		}
	}()

	for result := range queue {
		d := <-result
		if d.err != nil {
			return fmt.Errorf("decoding %s from block store: %w", d.filename, d.err)
		}
		if err := process(d.blk); err != nil {
			return err
		}
	}
	return nil
}
func (h *ForkableHub) WalkOneBlocksStore(ctx context.Context) ([]string, error) {
	sortedOneBlocksFiles := make([]string, 0)
	err := h.oneBlocksStore.Walk(
		ctx,
		"",
		func(filename string) error {
			sortedOneBlocksFiles = append(sortedOneBlocksFiles, filename)
			return nil
		})
	return sortedOneBlocksFiles, err
}
func (h *ForkableHub) WalkOneBlocksStoreFrom(ctx context.Context, startingBlock uint64) ([]string, error) {
	startingPoint := fmt.Sprintf("%010d", startingBlock)
	sortedOneBlocksFiles := make([]string, 0)
	err := h.oneBlocksStore.WalkFrom(
		ctx,
		"",
		startingPoint,
		func(filename string) error {
			sortedOneBlocksFiles = append(sortedOneBlocksFiles, filename)
			return nil
		})
	return sortedOneBlocksFiles, err
}

func decodeOneBlockFromFilename(ctx context.Context, filename string, store dstore.Store) (*pbbstream.Block, error) {
	reader, err := store.OpenObject(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("fetching %s from block store: %w", filename, err)
	}

	defer reader.Close()

	readerData, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading %s from block store: %w", filename, err)
	}

	return bstream.DecodeOneblockfileData(readerData)
}

// Notes: that function is called by the forkable when a block is processed
func (h *ForkableHub) broadcastBlock(blk *pbbstream.Block, obj any) error {
	h.logger.Debug("process_block", zap.Stringer("blk", blk.AsRef()), zap.Any("obj", obj.(*forkable.ForkableObject).Step()))

	// broadcastBlock is called on LIVE blocks only
	if liveable, ok := obj.(bstream.Liveable); ok {
		liveable.SetLiveBlock(true)
	}

	preprocBlock := &bstream.PreprocessedBlock{Block: blk, Obj: obj}

	subscribers := h.subscribers // we may remove some from the original slice during the loop

	for _, sub := range subscribers {
		err := sub.push(preprocBlock)
		if err != nil {
			h.unsubscribe(sub)
			sub.Shutdown(err)
		}

	}
	return nil
}

func (h *ForkableHub) reconnect(err error) {
	if errors.Is(err, errRestartRequired) {
		h.Shutdown(err)
		return
	}

	h.logger.Info("reconnecting hub after disconnection. expecting to reconnect",
		zap.Error(err))

	liveSource := h.liveSourceFactory(h)
	liveSource.OnTerminating(func(err error) {
		h.reconnect(err)
	})
	go liveSource.Run()
}

// substractAndRoundDownBlocks rounds down to a merged-blocks bundle boundary so
// the hub's lowest buffered block lines up with a file boundary, letting the
// joining source hand off from a merged-blocks file.
func substractAndRoundDownBlocks(blknum, sub, bundleSize uint64) uint64 {
	bundleSize = bstream.SanitizeBundleSize(bundleSize)

	var out uint64
	if blknum < sub {
		out = 0
	} else {
		out = blknum - sub
	}
	out = out / bundleSize * bundleSize

	if out < bstream.GetProtocolFirstStreamableBlock {
		return bstream.GetProtocolFirstStreamableBlock
	}

	return out
}
