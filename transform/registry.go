package transform

import (
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"sync"
)

type Factory struct {
	Obj     proto.Message
	NewFunc func(message *anypb.Any) (Transform, error)
}

type Registry struct {
	lock       sync.RWMutex
	transforms map[protoreflect.FullName]*Factory
}

func (r *Registry) Register(f *Factory) {
	r.lock.Lock()
	defer r.lock.Unlock()

	a, err := anypb.New(f.Obj)
	if err != nil {
		panic("unable to create any from proto message")
	}
	messageName := a.MessageName()
	if messageName == "" {
		panic("proto object message name cannot be blanked")
	} else if _, ok := r.transforms[messageName]; ok {
		panic(fmt.Sprintf("proto object message name %q already registered", a.TypeUrl))
	}
	r.transforms[messageName] = f
}

func (r *Registry) New(message *anypb.Any) (Transform, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	messageName := message.MessageName()
	factory, found := r.transforms[messageName]
	if !found {
		return nil, fmt.Errorf("no such transform registered %q", message.TypeUrl)
	}
	transform, err := factory.NewFunc(message)
	if err != nil {
		return nil, fmt.Errorf("unable to setup transform: %w", err)
	}
	return transform, nil
}
