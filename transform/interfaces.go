package transform

import (
	"context"

	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/bstream/stream"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Transform interface {
	String() string
}

type StreamGetter func(ctx context.Context, handler bstream.Handler, request *pbfirehose.Request, logger *zap.Logger) (*stream.Stream, error)
type StreamOutput func(*bstream.Cursor, *anypb.Any) error

type PassthroughTransform interface {
	Run(ctx context.Context, req *pbfirehose.Request, getStream StreamGetter, output StreamOutput) error
}

type PreprocessTransform interface {
	Transform(readOnlyBlk *pbbstream.Block, in Input) (Output, error)
}

type Input interface {
	Type() string
	Obj() proto.Message // Most of the time a `pbsol.Block` or `pbeth.Block`
}

type Output proto.Message

const (
	NilObjectType string = "nil"
)

type NilObj struct{}

func NewNilObj() *NilObj             { return &NilObj{} }
func (n *NilObj) Type() string       { return NilObjectType }
func (n *NilObj) Obj() proto.Message { return nil }

type InputObj struct {
	_type string
	obj   proto.Message
	ttl   int
}

func (i *InputObj) Obj() proto.Message {
	return i.obj
}

func (i *InputObj) Type() string {
	return i._type
}
