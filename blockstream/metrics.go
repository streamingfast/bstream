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

package blockstream

import (
	"github.com/streamingfast/dmetrics"
)

// metrics is registered when a block stream server is created, so a process exports these
// metrics only when it serves a block stream (e.g. a reader serving its relayers).
var metrics = dmetrics.NewSet(dmetrics.PrefixNameWith("bstream_blockstream"))

// SubscriptionsClosedFull counts subscriptions closed because their channel was full: the
// subscriber (e.g. a relayer) fell more than the subscription channel size behind and was
// disconnected. See ServerOptionWithSubscriptionChannelSize.
var SubscriptionsClosedFull = metrics.NewCounter("subscriptions_closed_full_total", "Number of block stream subscriptions closed because the subscriber fell more blocks behind than the subscription channel size")
