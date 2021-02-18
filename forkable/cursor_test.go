package forkable

import (
	"testing"

	"github.com/dfuse-io/bstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorFromString(t *testing.T) {
	ref := func(num uint64, id string) bstream.BlockRef {
		return bstream.NewBlockRef(id, num)
	}
	emptyRef := ref(0, "")

	tests := []struct {
		name        string
		in          string
		expected    *Cursor
		expectedErr error
	}{
		{
			"c1 no LIB",
			"c1:1:11846516:d46eeb12ad30ef291a673329bb2f64bc689f15253371b64aaee017556ee95a35:0:",
			&Cursor{
				Step:      StepNew,
				Block:     ref(11846516, "d46eeb12ad30ef291a673329bb2f64bc689f15253371b64aaee017556ee95a35"),
				HeadBlock: ref(11846516, "d46eeb12ad30ef291a673329bb2f64bc689f15253371b64aaee017556ee95a35"),
				LIB:       emptyRef,
			},
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := CursorFromString(test.in)
			if test.expectedErr == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expected, actual)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
		})
	}
}
