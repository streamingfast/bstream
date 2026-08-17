package stream

import (
	"errors"
	"fmt"
)

type ErrInvalidArg struct {
	message string
}

func NewErrInvalidArg(m string, args ...any) *ErrInvalidArg {
	return &ErrInvalidArg{
		message: fmt.Sprintf(m, args...),
	}
}

func (e *ErrInvalidArg) Error() string {
	return e.message
}

// ErrUnavailable is a request this process could not serve yet, for servers to map to
// their transport's retryable status (gRPC codes.Unavailable).
type ErrUnavailable struct {
	message string
}

func NewErrUnavailable(m string, args ...any) *ErrUnavailable {
	return &ErrUnavailable{
		message: fmt.Sprintf(m, args...),
	}
}

func (e *ErrUnavailable) Error() string {
	return e.message
}

var ErrStopBlockReached = errors.New("stop block reached")
