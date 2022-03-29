package transform

import (
	"context"

	"github.com/streamingfast/bstream"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type Transform interface {
	String() string
}

type PassthroughTransform interface {
	Run(ctx context.Context, req *pbfirehose.Request, output func(*bstream.Cursor, *anypb.Any) error) error
}

type PreprocessTransform interface {
	Transform(readOnlyBlk *bstream.Block, in Input) (Output, error)
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
