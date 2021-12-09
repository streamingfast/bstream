package transform

import (
	"fmt"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type NewTransformFunc func(p proto.Message) (Transform, error)

var registry = make(map[protoreflect.FullName]NewTransformFunc)

func Register(name protoreflect.FullName, f NewTransformFunc) {
	if name == "" {
		zlog.Fatal("name cannot be blank")
	} else if _, ok := registry[name]; ok {
		zlog.Fatal("already registered", zap.String("name", string(name)))
	}
	registry[name] = f
}

func New(message proto.Message) (Transform, error) {
	key := proto.MessageName(message)
	newFunc, found := registry[key]
	if !found {
		return nil, fmt.Errorf("no such transform registered %q", key)
	}
	transform, err := newFunc(message)
	if err != nil {
		return nil, fmt.Errorf("unable to setup transform: %w", err)
	}
	return transform, nil
}
