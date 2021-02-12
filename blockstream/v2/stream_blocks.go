package blockstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/dfuse-io/bstream/firehose"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/forkable"
	"github.com/dfuse-io/logging"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStopBlockReached = errors.New("stop block reached")

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

	options := []firehose.Option{
		firehose.WithLogger(s.logger),
		firehose.WithForkableSteps(forkable.StepsFromProto(request.ForkSteps)),
		firehose.WithLiveHeadTracker(s.liveHeadTracker),
		firehose.WithTracker(s.tracker),
		firehose.WithStopBlock(request.StopBlockNum),
	}

	if request.StartCursor != "" {
		cur, err := forkable.CursorFromOpaque(request.StartCursor)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "invalid start cursor %q: %s", request.StartCursor, err)
		}
		options = append(options, firehose.WithCursor(cur))
	}

	if s.preprocFactory != nil {
		preproc, err := s.preprocFactory(request)
		if err != nil {
			return status.Errorf(codes.Internal, "unable to create preproc function: %s", err)
		}
		options = append(options, firehose.WithPreproc(preproc))
	}


	if s.liveSourceFactory != nil {
		options = append(options, firehose.WithLiveSource(s.liveSourceFactory))
	}

	fhose := firehose.New(s.blocksStores, request.StartBlockNum, handlerFunc, options...)


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

