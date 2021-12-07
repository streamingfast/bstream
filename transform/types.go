package transform

import "google.golang.org/protobuf/proto"

type input struct {
	typ      string
	memoized proto.Message
	payload  []byte
}

type Input interface {
	Type() string
	Obj() proto.Message // Most of the time a `pbsol.Block` or `pbeth.Block`
}

type Output proto.Message
