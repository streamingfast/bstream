package bstream

/**

Usage:

	a.tracker.AddGetter(bstream.StreamHeadTarget, dexer.Hub)
	a.tracker.AddGetter(bstream.StreamHeadTarget, timeline.ParallelGetter(relayerClient.GetterFactory(a.config.RelayerAddr), blockmeta.TrackerGetterFactory(a.config.BlockmetaAddr)))
	a.tracker.AddGetter(bstream.StreamHeadTarget, codec.GetterFactory(eosAPI))
	//a.tracker.AddGetter(dmesh.ArchiveHeadTarget, dmesh.TrackerGetterFactory(dmeshServer))
	// dmesh/interface.go
	// const ArchiveTailTarget = dtrack.Target("archive-tail")
	blockRef, err := a.tracker.Latest(dmesh.ArchiveTailTarget)

REVIEW THE ABOVE.. find a nice pattern ^^
*/

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

//
// Types to support the Tracker
//

// BlockRefGetter is a function to retrieve a block ref from any system.
type BlockRefGetter func(context.Context) (BlockRef, error)

var ErrTrackerBlockNotFound = errors.New("tracker block not found")
var ErrGetterUndefined = errors.New("no getter defined")

type Target string

const (
	FileSourceHeadTarget  = Target("filesource-head")
	LiveSourceHeadTarget  = Target("livesource-head")
	LiveSourceTailTarget  = Target("livesource-tail")
	NetworkHeadTarget     = Target("network-head")
	NetworkLIBTarget      = Target("network-lib")
	BlockStreamHeadTarget = Target("bstream-head")
	BlockStreamLIBTarget  = Target("bstream-lib")
	HubHeadTarget         = Target("hub-head")
	HubLIBTarget          = Target("hub-lib")
	// DatabaseHeadTarget    = Target("db-head")
	// DmeshTailTarget       = Target("dmesh-tail")
	// DmeshHeadTarget       = Target("dmesh-head")
)

// Tracker tracks the chain progress and block history. Allows many
// processes to take decisions on the state of different pieces being
// sync'd (live) or in catch-up mode.
type Tracker struct {
	getters map[Target][]BlockRefGetter

	// Number of blocks between two targets before we consider the
	// first as "near" the second. Like a relayer stream being near
	// the tip of the network.
	nearBlocksCount int64
}

func NewTracker(nearBlocksCount uint64) *Tracker {
	return &Tracker{
		getters:         make(map[Target][]BlockRefGetter),
		nearBlocksCount: int64(nearBlocksCount),
	}
}

func (t *Tracker) AddGetter(target Target, f BlockRefGetter) {
	t.getters[target] = append(t.getters[target], f)
}

func (t *Tracker) SetNearBlocksCount(count int64) {
	t.nearBlocksCount = count
}

func (t *Tracker) Clone() *Tracker {
	dstGettersMap := map[Target][]BlockRefGetter{}

	for k, srcGetters := range t.getters {
		var dstGetters []BlockRefGetter
		for _, getter := range srcGetters {
			dstGetters = append(dstGetters, getter)
		}
		dstGettersMap[k] = dstGetters
	}

	return &Tracker{
		nearBlocksCount: t.nearBlocksCount,
		getters:         dstGettersMap,
	}
}

// IsNearManualCheck allows you to manually check two "already resolved values" for nearness.
func (t *Tracker) IsNearManualCheck(from, to uint64) bool {
	return to < uint64(t.nearBlocksCount) || int64(to)-int64(from) <= t.nearBlocksCount
}

// IsNearWithResults returns BlockRefs for the two targets.  It can short-circuit the lookup for `from` if `to` is near the beginning of the chain, within the `nearBlocksCount`, in which case `fromBlockRef` will be nil.
func (t *Tracker) IsNearWithResults(ctx context.Context, from Target, to Target) (fromBlockRef BlockRef, toBlockRef BlockRef, isNear bool, err error) {
	toBlockRef, err = t.Get(ctx, to)
	if err != nil {
		return
	}

	if toBlockRef.Num() < uint64(t.nearBlocksCount) {
		// Near the beginning of the chain!
		isNear = true
		return
	}

	fromBlockRef, err = t.Get(ctx, from)
	if err != nil {
		return
	}

	if int64(toBlockRef.Num())-int64(fromBlockRef.Num()) <= t.nearBlocksCount {
		isNear = true
	}

	return
}

func (t *Tracker) IsNear(ctx context.Context, from Target, to Target) (bool, error) {
	_, _, result, err := t.IsNearWithResults(ctx, from, to)
	return result, err
}

func (t *Tracker) Get(ctx context.Context, target Target) (BlockRef, error) {
	getters := t.getters[target]
	if len(getters) == 0 {
		return nil, fmt.Errorf("tracker getter for target %q: %w", target, ErrGetterUndefined)
	}
	var errs []string
	for _, f := range getters {
		ref, err := f(ctx)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		return ref, nil
	}

	return nil, errors.New("tracking fetch errors in order: " + strings.Join(errs, ", "))

	// Get from EOS API
	// Get from HeadInfo from any service ()
	// Get from SubscriptionHub  (use `tracker.AddGetter(bstream.StreamHeadTarget, hub.HeadTracker)`)
	// Get from blockmeta
	// Get from blkdb/trxdb's last written block
	// Get from fluxdb's latest written block
	// Get from command-line checkpoint?
}

func (t *Tracker) GetRelativeBlock(ctx context.Context, potentiallyNegativeBlockNum int64, target Target) (uint64, error) {
	if potentiallyNegativeBlockNum < 0 {
		blk, err := t.Get(ctx, target)
		if err != nil {
			return 0, err
		}

		if blk.Num() < uint64(-potentiallyNegativeBlockNum) {
			return GetProtocolFirstStreamableBlock, nil
		}

		return uint64(int64(blk.Num()) + potentiallyNegativeBlockNum), nil
	}
	if uint64(potentiallyNegativeBlockNum) < GetProtocolFirstStreamableBlock {
		return GetProtocolFirstStreamableBlock, nil
	}
	return uint64(potentiallyNegativeBlockNum), nil
}

func HighestBlockRefGetter(getters ...BlockRefGetter) BlockRefGetter {
	type resp struct {
		ref BlockRef
		err error
	}

	return func(ctx context.Context) (BlockRef, error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		outChan := make(chan *resp)
		for _, getter := range getters {
			g := getter
			go func() {
				ref, err := g(ctx)
				resp := &resp{
					ref: ref,
					err: err,
				}
				select {
				case outChan <- resp:
				case <-ctx.Done():
				}
			}()
		}

		var errs []string
		var highest BlockRef
		for cnt := 0; cnt < len(getters); cnt++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case resp := <-outChan:
				if resp.err != nil {
					errs = append(errs, resp.err.Error())
				} else {
					if highest == nil {
						highest = resp.ref
					} else {
						if resp.ref.Num() > highest.Num() {
							highest = resp.ref
						}
					}
				}
			}
		}

		if len(errs) == len(getters) {
			return nil, errors.New("all parallel getters failed: " + strings.Join(errs, ", "))
		}

		return highest, nil
	}
}

func RetryableBlockRefGetter(attempts int, wait time.Duration, next BlockRefGetter) BlockRefGetter {
	return func(ctx context.Context) (ref BlockRef, err error) {
		var errs []string
		for attempt := 0; attempts == -1 || attempt <= attempts; attempt++ {
			if err = ctx.Err(); err != nil {
				errs = append(errs, err.Error())
				break
			}

			ref, err = next(ctx)
			if err != nil {
				zlog.Debug("got an error from a block ref getter", zap.Error(err))
				errs = append(errs, err.Error())
				attempt++
				time.Sleep(wait)
				continue
			}
			return
		}
		err = fmt.Errorf("retryable block ref getter failed: %s", strings.Join(errs, ", "))
		return
	}
}
