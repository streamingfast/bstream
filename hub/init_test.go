package hub

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func init() {
	logger := logging.MustCreateLoggerWithLevel("test", zap.NewAtomicLevelAt(zap.DebugLevel))
	logging.Override(logger)
}

func assertExpectedBlocks(t *testing.T, expected, actual []expectedBlock) {
	t.Helper()

	require.Equal(t, len(expected), len(actual))
	for idx, expect := range expected {
		assertExpectedBlock(t, expect, actual[idx])
	}
}

func assertExpectedBlock(t *testing.T, expected, actual expectedBlock) {
	t.Helper()

	assert.Equal(t, expected.step, actual.step)
	assert.Equal(t, expected.cursorLibNum, actual.cursorLibNum)
	bstream.AssertProtoEqual(t, expected.block, actual.block)
}
