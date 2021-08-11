package firehose

import (
	"github.com/streamingfast/logging"
	"go.uber.org/zap"
)

var traceEnabled = logging.IsTraceEnabled("bstream", "github.com/streamingfast/bstream/firehose")
var zlog *zap.Logger

func init() {
	logging.Register("github.com/streamingfast/bstream/firehose", &zlog)
}
