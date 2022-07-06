package hub

import (
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

func init() {
	logger := logging.MustCreateLoggerWithLevel("test", zap.NewAtomicLevelAt(zap.DebugLevel))
	logging.Override(logger)
}
