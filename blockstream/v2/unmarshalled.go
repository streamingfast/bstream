package blockstream

import (
	"context"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbany "github.com/golang/protobuf/ptypes/any"
	"github.com/lytics/ordpool"
	"go.uber.org/zap"
)

func GetUnmarshalledBlockClient(blocksClient pbbstream.BlockStreamV2_BlocksClient, unmarshaller func(in *pbany.Any) interface{}) *UnmarshalledBlocksClient {
	return &UnmarshalledBlocksClient{
		cli:          blocksClient,
		unmarshaller: unmarshaller,
	}
}

type UnmarshalledBlocksClient struct {
	cli          pbbstream.BlockStreamV2_BlocksClient
	unmarshaller func(*pbany.Any) interface{}
}

func (ubc *UnmarshalledBlocksClient) Recv() (*UnmarshalledBlocksResponseV2, error) {
	out := &UnmarshalledBlocksResponseV2{}
	in, err := ubc.cli.Recv()
	if in != nil {
		out.Block = ubc.unmarshaller(in.Block)
		out.Cursor = in.Cursor
		out.Step = in.Step
	}
	return out, err
}

type localUnmarshalledBlocksClient struct {
	in        <-chan interface{}
	lastError error
}

func (lubc *localUnmarshalledBlocksClient) Recv() (*UnmarshalledBlocksResponseV2, error) {
	in, ok := <-lubc.in
	if !ok {
		return nil, lubc.lastError
	}
	return in.(*UnmarshalledBlocksResponseV2), nil
}

func unmarshalBlock(input interface{}) (interface{}, error) {
	resp := input.(*UnmarshalledBlocksResponseV2)
	resp.Block = resp.Block.(*bstream.Block).ToNative()
	return resp, nil
}

func (s Server) UnmarshalledBlocksFromLocal(ctx context.Context, req *pbbstream.BlocksRequestV2) *localUnmarshalledBlocksClient {
	o := ordpool.New(StreamBlocksParallelThreads, unmarshalBlock)
	o.Start()
	toUnmarshaller := o.GetInputCh()

	lubc := &localUnmarshalledBlocksClient{
		in: o.GetOutputCh(),
	}

	go func() {
		err := s.unmarshalledBlocks(ctx, req, toUnmarshaller)
		lubc.lastError = err
		o.Stop()
	}()

	return lubc

}

func (s Server) unmarshalledBlocks(ctx context.Context, request *pbbstream.BlocksRequestV2, pipe chan<- interface{}) error {
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		fObj := obj.(*forkable.ForkableObject)

		resp := &UnmarshalledBlocksResponseV2{
			Block:  block,
			Step:   forkable.StepToProto(fObj.Step),
			Cursor: fObj.Cursor().ToOpaque(),
		}
		pipe <- resp
		return nil
	})

	return s.runBlocks(ctx, handlerFunc, request, logger)
}
