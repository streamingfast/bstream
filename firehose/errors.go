package firehose

import (
	"errors"
	"fmt"
)

type ErrInvalidArg struct {
	message string
}

func NewErrInvalidArg(m string, args ...interface{}) *ErrInvalidArg {
	return &ErrInvalidArg{
		message: fmt.Sprintf(m, args...),
	}
}

func (e *ErrInvalidArg) Error() string {
	return e.message
}

var ErrStopBlockReached = errors.New("stop block reached")
