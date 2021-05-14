package blockstream

import (
	"context"
	"errors"
	"fmt"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/firehose"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbany "github.com/golang/protobuf/ptypes/any"
	"github.com/lytics/ordpool"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var errStopBlockReached = errors.New("stop block reached")

type BlocksPipe struct {
	//grpc.ServerStream
	grpc.ClientStream
	ctx      context.Context
	pipeChan chan *pbbstream.BlockResponseV2
	err      error
}

func (p *BlocksPipe) SendHeader(metadata.MD) error {
	return nil
}
func (p *BlocksPipe) SetHeader(metadata.MD) error {
	return nil
}
func (p *BlocksPipe) SetTrailer(metadata.MD) {
	return
}

func (p *BlocksPipe) Context() context.Context {
	return p.ctx
}

func (p *BlocksPipe) Send(resp *pbbstream.BlockResponseV2) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.pipeChan <- resp:
	}
	return nil
}

func (p *BlocksPipe) Recv() (*pbbstream.BlockResponseV2, error) {
	select {
	case resp, ok := <-p.pipeChan:
		if !ok {
			return resp, p.err
		}
		return resp, nil
	case <-p.ctx.Done():
		select {
		// ensure we empty the pipeChan
		case resp, ok := <-p.pipeChan:
			if !ok {
				return resp, p.err
			}
			return resp, nil
		default:
			return nil, p.err
		}
	}
}

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

	var preprocFunc bstream.PreprocessFunc
	if s.preprocFactory != nil {
		pp, err := s.preprocFactory(request)
		if err != nil {
			return status.Errorf(codes.Internal, "unable to create preproc function: %s", err)
		}
		preprocFunc = pp
	}

	var fileSourceOptions []bstream.FileSourceOption
	if len(s.blocksStores) > 1 {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(s.blocksStores[1:]))
	}
	fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(StreamBlocksParallelThreads))

	fileSourceFactory := bstream.SourceFromNumFactory(func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStores[0],
			startBlockNum,
			StreamBlocksParallelFiles,
			preprocFunc,
			h,
			fileSourceOptions...,
		)
		return fs
	})

	options := []firehose.Option{
		firehose.WithLogger(s.logger),
		firehose.WithForkableSteps(forkable.StepsFromProto(request.ForkSteps)),
		firehose.WithLiveHeadTracker(s.liveHeadTracker),
		firehose.WithTracker(s.tracker),
		firehose.WithStopBlock(request.StopBlockNum),
	}

	if request.Confirmations != 0 {
		options = append(options, firehose.WithConfirmations(request.Confirmations))
	}

	if request.StartCursor != "" {
		cur, err := forkable.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		options = append(options, firehose.WithCursor(cur))
	}

	if s.liveSourceFactory != nil {
		liveFactory := s.liveSourceFactory

		if preprocFunc != nil {
			liveFactory = func(h bstream.Handler) bstream.Source {
				newHandler := bstream.NewPreprocessor(preprocFunc, h)
				return s.liveSourceFactory(bstream.CloneBlock(newHandler)) // we clone ourself so no need for isolateConsumers
			}
		}
		options = append(options, firehose.WithLiveSource(liveFactory, false))
	}

	fhose := firehose.New(fileSourceFactory, request.StartBlockNum, handlerFunc, options...)

	err := fhose.Run(ctx)
	if err != nil {
		if errors.Is(err, firehose.ErrStopBlockReached) {
			logger.Info("stream of blocks reached end block")
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, "source canceled")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, "source deadline exceeded")
		}

		var e *firehose.ErrInvalidArg
		if errors.As(err, &e) {
			return status.Error(codes.InvalidArgument, e.Error())
		}

		logger.Info("unexpected stream of blocks termination", zap.Error(err))
		return status.Errorf(codes.Internal, "unexpected stream termination")
	}

	logger.Error("source is not expected to terminate gracefully, should stop at block or continue forever")
	return status.Error(codes.Internal, "unexpected stream completion")
}

func (s Server) BlocksFromLocal(ctx context.Context, req *pbbstream.BlocksRequestV2) pbbstream.BlockStreamV2_BlocksClient {
	cctx, cancel := context.WithCancel(ctx)

	pipe := &BlocksPipe{
		ctx:      cctx,
		pipeChan: make(chan *pbbstream.BlockResponseV2),
	}
	go func() {
		err := s.Blocks(req, pipe)
		pipe.err = err
		cancel()
	}()

	return pipe
}

func (s Server) Blocks(request *pbbstream.BlocksRequestV2, stream pbbstream.BlockStreamV2_BlocksServer) error {
	ctx := stream.Context()
	logger := logging.Logger(ctx, s.logger)
	logger.Info("incoming blocks request", zap.Reflect("req", request))

	var blockInterceptor func(blk interface{}) interface{}
	if s.trimmer != nil {
		blockInterceptor = func(blk interface{}) interface{} { return s.trimmer.Trim(blk, request.Details) }
	}

	handlerFunc := bstream.HandlerFunc(func(block *bstream.Block, obj interface{}) error {
		any, err := block.ToAny(true, blockInterceptor)
		if err != nil {
			return fmt.Errorf("to any: %w", err)
		}
		fObj := obj.(*forkable.ForkableObject)

		resp := &pbbstream.BlockResponseV2{
			Block:  any,
			Step:   forkable.StepToProto(fObj.Step),
			Cursor: fObj.Cursor().ToOpaque(),
		}
		if s.postHookFunc != nil {
			s.postHookFunc(ctx, resp)
		}
		err = stream.Send(resp)
		if err != nil {
			return err
		}

		return nil
	})

	var preprocFunc bstream.PreprocessFunc
	if s.preprocFactory != nil {
		pp, err := s.preprocFactory(request)
		if err != nil {
			return status.Errorf(codes.Internal, "unable to create preproc function: %s", err)
		}
		preprocFunc = pp
	}

	var fileSourceOptions []bstream.FileSourceOption
	if len(s.blocksStores) > 1 {
		fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithSecondaryBlocksStores(s.blocksStores[1:]))
	}
	fileSourceOptions = append(fileSourceOptions, bstream.FileSourceWithConcurrentPreprocess(StreamBlocksParallelThreads)) //

	fileSourceFactory := bstream.SourceFromNumFactory(func(startBlockNum uint64, h bstream.Handler) bstream.Source {
		fs := bstream.NewFileSource(
			s.blocksStores[0],
			startBlockNum,
			StreamBlocksParallelFiles,
			preprocFunc,
			h,
			fileSourceOptions...,
		)
		return fs
	})

	options := []firehose.Option{
		firehose.WithLogger(s.logger),
		firehose.WithForkableSteps(forkable.StepsFromProto(request.ForkSteps)),
		firehose.WithLiveHeadTracker(s.liveHeadTracker),
		firehose.WithTracker(s.tracker),
		firehose.WithStopBlock(request.StopBlockNum),
	}

	if request.Confirmations != 0 {
		options = append(options, firehose.WithConfirmations(request.Confirmations))
	}

	if request.StartCursor != "" {
		cur, err := forkable.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}

		options = append(options, firehose.WithCursor(cur))
	}

	if s.liveSourceFactory != nil {
		liveFactory := s.liveSourceFactory

		if preprocFunc != nil {
			liveFactory = func(h bstream.Handler) bstream.Source {
				newHandler := bstream.NewPreprocessor(preprocFunc, h)
				return s.liveSourceFactory(bstream.CloneBlock(newHandler)) // we clone ourself so no need for isolateConsumers
			}
		}
		options = append(options, firehose.WithLiveSource(liveFactory, false))
	}

	fhose := firehose.New(fileSourceFactory, request.StartBlockNum, handlerFunc, options...)

	err := fhose.Run(ctx)
	if err != nil {
		if errors.Is(err, firehose.ErrStopBlockReached) {
			logger.Info("stream of blocks reached end block")
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return status.Error(codes.Canceled, "source canceled")
		}

		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, "source deadline exceeded")
		}

		var e *firehose.ErrInvalidArg
		if errors.As(err, &e) {
			return status.Error(codes.InvalidArgument, e.Error())
		}

		logger.Info("unexpected stream of blocks termination", zap.Error(err))
		return status.Errorf(codes.Internal, "unexpected stream termination")
	}

	logger.Error("source is not expected to terminate gracefully, should stop at block or continue forever")
	return status.Error(codes.Internal, "unexpected stream completion")
}
