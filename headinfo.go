package bstream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/streamingfast/dgrpc"
	pbheadinfo "github.com/streamingfast/pbgo/sf/headinfo/v1"
)

func NetworkLIBBlockRefGetter(headinfoServiceAddr string) BlockRefGetter {
	return blockRefGetter(headinfoServiceAddr, pbheadinfo.HeadInfoRequest_NETWORK, func(resp *pbheadinfo.HeadInfoResponse) BlockRef {
		return &BasicBlockRef{id: resp.LibID, num: resp.LibNum}
	})
}

func NetworkHeadBlockRefGetter(headinfoServiceAddr string) BlockRefGetter {
	return blockRefGetter(headinfoServiceAddr, pbheadinfo.HeadInfoRequest_NETWORK, func(resp *pbheadinfo.HeadInfoResponse) BlockRef {
		return &BasicBlockRef{id: resp.HeadID, num: resp.HeadNum}
	})
}

func StreamLIBBlockRefGetter(headinfoServiceAddr string) BlockRefGetter {
	return blockRefGetter(headinfoServiceAddr, pbheadinfo.HeadInfoRequest_STREAM, func(resp *pbheadinfo.HeadInfoResponse) BlockRef {
		return &BasicBlockRef{id: resp.LibID, num: resp.LibNum}
	})
}

func StreamHeadBlockRefGetter(headinfoServiceAddr string) BlockRefGetter {
	return blockRefGetter(headinfoServiceAddr, pbheadinfo.HeadInfoRequest_STREAM, func(resp *pbheadinfo.HeadInfoResponse) BlockRef {
		return &BasicBlockRef{id: resp.HeadID, num: resp.HeadNum}
	})
}

func GetStreamHeadInfo(ctx context.Context, addr string) (head BlockRef, lib BlockRef, err error) {
	cli, err := headInfoClient(addr)
	if err != nil {
		return nil, nil, err
	}

	resp, err := cli.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{Source: pbheadinfo.HeadInfoRequest_STREAM})
	if err != nil {
		return nil, nil, err
	}

	return NewBlockRef(resp.HeadID, resp.HeadNum), NewBlockRef(resp.LibID, resp.LibNum), nil

}

func headInfoClient(addr string) (pbheadinfo.HeadInfoClient, error) {
	conn, err := dgrpc.NewInternalClient(addr)
	if err != nil {
		return nil, fmt.Errorf("reaching out to headinfo service %q: %w", addr, err)
	}
	return pbheadinfo.NewHeadInfoClient(conn), nil
}

func blockRefGetter(headinfoServiceAddr string, source pbheadinfo.HeadInfoRequest_Source, extract func(resp *pbheadinfo.HeadInfoResponse) BlockRef) BlockRefGetter {
	var lock sync.Mutex
	var headinfoCli pbheadinfo.HeadInfoClient

	return func(ctx context.Context) (BlockRef, error) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		lock.Lock()
		defer lock.Unlock()

		if headinfoCli == nil {
			cli, err := headInfoClient(headinfoServiceAddr)
			if err != nil {
				return nil, err
			}
			headinfoCli = cli
		}

		resp, err := headinfoCli.GetHeadInfo(ctx, &pbheadinfo.HeadInfoRequest{Source: source})
		if err == nil && resp.HeadNum != 0 {
			return extract(resp), nil
		}

		// TODO: distinguish the remote `NotFound` and another error, return `NotFound` is it was
		// indeed not found, and its not another type of error.
		return nil, ErrTrackerBlockNotFound

	}
}
