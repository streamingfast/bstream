package firehose

import (
	"github.com/dfuse-io/logging"
	"go.uber.org/zap"
)

var traceEnabled = logging.IsTraceEnabled("bstream", "github.com/dfuse-io/bstream/firehose")
var zlog *zap.Logger

func init() {
	logging.Register("github.com/dfuse-io/bstream/firehose", &zlog)
}
