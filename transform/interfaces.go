package transform

import (
	"github.com/streamingfast/bstream"
	"google.golang.org/protobuf/proto"
)

type Transform interface{}

type ParallelTransform interface {
	PreprocessBlock(readOnlyBlk *bstream.Block, in Input) (Output, error)
}

type LinearTransform interface {
	ProcessBlock(readOnlyBlk *bstream.Block, step bstream.StepType) (Output, error)
}

type TransformWithStartBlock interface {
	ResolveStartBlock(uint64) uint64
}

type StartBlockResolver func(uint64) uint64

type ProcessBlockFunc func(readOnlyBlk *bstream.Block, step bstream.StepType) (Output, error)

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
