package bstream

import (
	"context"
	"fmt"
	"sync"

	"github.com/dfuse-io/dgrpc"
	pbheadinfo "github.com/dfuse-io/pbgo/dfuse/headinfo/v1"
)

func LIBBlockRefGetter(headinfoServiceAddr string) BlockRefGetter {
	return blockRefGetter(headinfoServiceAddr, func(resp *pbheadinfo.HeadInfoResponse) BlockRef {
		return &BasicBlockRef{
			id:  resp.LibID,
			num: resp.LibNum,
		}
	})
}

func HeadBlockRefGetter(headinfoServiceAddr string) BlockRefGetter {
	return blockRefGetter(headinfoServiceAddr, func(resp *pbheadinfo.HeadInfoResponse) BlockRef {
		return &BasicBlockRef{
			id:  resp.HeadID,
			num: resp.HeadNum,
		}
	})
}

func blockRefGetter(headinfoServiceAddr string, extract func(resp *pbheadinfo.HeadInfoResponse) BlockRef) BlockRefGetter {
	var lock sync.Mutex
	var headinfoCli pbheadinfo.HeadInfoClient

	return func(ctx context.Context) (BlockRef, error) {
		lock.Lock()
		defer lock.Unlock()

		if headinfoCli == nil {
			conn, err := dgrpc.NewInternalClient(headinfoServiceAddr)
			if err != nil {
				return nil, fmt.Errorf("reaching out to headinfo service %q: %w", headinfoServiceAddr, err)
			}
			headinfoCli = pbheadinfo.NewHeadInfoClient(conn)
		}

		// TODO: implement the `NETWORK` and `STREAM` and `DB` query kinds, for when we query
		// blockmeta, which can answer all of those things.
		resp, err := headinfoCli.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{})
		if err == nil && resp.HeadNum != 0 {
			return extract(resp), nil
		}

		// TODO: distinguish the remote `NotFound` and another error, return `NotFound` is it was
		// indeed not found, and its not another type of error.
		return nil, ErrTrackerBlockNotFound

	}
}
