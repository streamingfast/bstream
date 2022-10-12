// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

import (
	"github.com/streamingfast/dmetrics"
)

var Metrics = dmetrics.NewSet(dmetrics.PrefixNameWith("bstream"))

var BlocksReadFileSource = Metrics.NewCounter("blocks_read_filesource", "Number of blocks read from file source")
var BytesReadFileSource = Metrics.NewCounter("bytes_read_filesource", "Bytes read from file source")

var BlocksSentFileSource = Metrics.NewCounter("blocks_sent_filesource", "Number of blocks sent that came from file source")
var BytesSentFileSource = Metrics.NewCounter("bytes_sent_filesource", "Bytes sent that came from file source")

var BlocksReadLiveSource = Metrics.NewCounter("blocks_read_livesource", "Number of blocks read from live source")
var BytesReadLiveSource = Metrics.NewCounter("bytes_read_livesource", "Bytes read from live source")

var BlocksBehindLive = Metrics.NewGaugeVec("blocks_behind_live", []string{"trace_id"}, "Number of blocks behind live source")

func WithHeadMetrics(h Handler, blkNum *dmetrics.HeadBlockNum, blkDrift *dmetrics.HeadTimeDrift) Handler {
	return HandlerFunc(func(blk *Block, obj interface{}) error {
		blkDrift.SetBlockTime(blk.Time())
		blkNum.SetUint64(blk.Number)
		return h.ProcessBlock(blk, obj)
	})
}
