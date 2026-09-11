package hub

import (
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/require"
)

func TestSourceChanSizeFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		setEnv   bool
		expect   int
	}{
		{"unset keeps default", "", false, 100},
		{"empty keeps default", "", true, 100},
		{"garbage keeps default", "garbage", true, 100},
		{"valid value is used", "42", true, 42},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.setEnv {
				t.Setenv("SOURCE_CHAN_SIZE", test.envValue)
			}
			require.Equal(t, test.expect, sourceChanSizeFromEnv(100))
		})
	}
}

func TestNewForkableHub_InvalidSourceChanSizeEnv(t *testing.T) {
	t.Setenv("SOURCE_CHAN_SIZE", "not-a-number")

	lsf := bstream.NewTestSourceFactory()
	fh := NewForkableHub(lsf.NewSource, 0, nil)

	require.Equal(t, 100, fh.sourceChannelSize,
		"invalid SOURCE_CHAN_SIZE must keep the default channel size instead of using 0 and disconnecting every subscriber")
}
