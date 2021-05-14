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
	out := &UnmarshalledBlocksClient{
		cli:          blocksClient,
		unmarshaller: unmarshaller,
	}
	out.Start()
	return out
}

type UnmarshalledBlocksClient struct {
	cli          pbbstream.BlockStreamV2_BlocksClient
	unmarshaller func(*pbany.Any) interface{}
	outputCh     <-chan interface{}
	lastError    error
}

func (ubc *UnmarshalledBlocksClient) Start() {
	o := ordpool.New(StreamBlocksParallelThreads,
		func(in interface{}) (interface{}, error) {
			inResp := in.(*pbbstream.BlockResponseV2)
			out := &UnmarshalledBlocksResponseV2{
				Block:  ubc.unmarshaller(inResp.Block),
				Cursor: inResp.Cursor,
				Step:   inResp.Step,
			}
			return out, nil
		})
	o.Start()
	ubc.outputCh = o.GetOutputCh()
	toUnmarshaller := o.GetInputCh()
	go func() {
		defer func() {
			o.Stop()
			o.WaitForShutdown()
		}()
		for {
			in, err := ubc.cli.Recv()
			if err != nil {
				ubc.lastError = err
				return
			}
			toUnmarshaller <- in
		}
	}()
}

func (ubc *UnmarshalledBlocksClient) Recv() (*UnmarshalledBlocksResponseV2, error) {
	in, ok := <-ubc.outputCh
	if !ok {
		return nil, ubc.lastError
	}
	return in.(*UnmarshalledBlocksResponseV2), nil
}

type localUnmarshalledBlocksClient struct {
	pipe      <-chan interface{}
	lastError error
}

func (lubc *localUnmarshalledBlocksClient) Recv() (*UnmarshalledBlocksResponseV2, error) {
	in, ok := <-lubc.pipe
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
		pipe: o.GetOutputCh(),
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
