package transform

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

type NewTransformFunc func(message *anypb.Any) (Transform, error)

var registry = make(map[protoreflect.FullName]NewTransformFunc)

func Register(protoMessage proto.Message, f func(message *anypb.Any) (Transform, error)) {
	a, err := anypb.New(protoMessage)
	if err != nil {
		panic("unable to create any from proto message")
	}
	messageName := a.MessageName()
	if messageName == "" {
		panic("proto object message name cannot be blanked")
	} else if _, ok := registry[messageName]; ok {
		panic(fmt.Sprintf("proto object message name %q already registered", a.TypeUrl))
	}
	registry[messageName] = f
}

func New(message *anypb.Any) (Transform, error) {
	messageName := message.MessageName()
	newFunc, found := registry[messageName]
	if !found {
		return nil, fmt.Errorf("no such transform registered %q", message.TypeUrl)
	}
	transform, err := newFunc(message)
	if err != nil {
		return nil, fmt.Errorf("unable to setup transform: %w", err)
	}
	return transform, nil
}
