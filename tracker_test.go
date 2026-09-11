package bstream

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryableBlockRefGetter_AttemptCount(t *testing.T) {
	tests := []struct {
		name          string
		attempts      int
		succeedOnCall int // 0 means never succeed
		expectCalls   int
		expectError   bool
	}{
		{"always failing consumes each configured attempt", 4, 0, 4, true},
		{"single attempt", 1, 0, 1, true},
		{"succeeds on first call", 4, 1, 1, false},
		{"succeeds after two failures", 4, 3, 3, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var calls int
			getter := BlockRefGetter(func(ctx context.Context) (BlockRef, error) {
				calls++
				if test.succeedOnCall != 0 && calls >= test.succeedOnCall {
					return NewBlockRef("00000001", 1), nil
				}
				return nil, fmt.Errorf("failure %d", calls)
			})

			ref, err := RetryableBlockRefGetter(test.attempts, 0, getter)(context.Background())

			assert.Equal(t, test.expectCalls, calls)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, ref)
				assert.EqualValues(t, 1, ref.Num())
			}
		})
	}
}

func TestRetryableBlockRefGetter_CanceledContext(t *testing.T) {
	var calls int
	getter := BlockRefGetter(func(ctx context.Context) (BlockRef, error) {
		calls++
		return nil, fmt.Errorf("failure")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := RetryableBlockRefGetter(4, 0, getter)(ctx)
	require.Error(t, err)
	assert.Equal(t, 0, calls, "canceled context must prevent any call to the wrapped getter")
}
