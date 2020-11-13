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

package hub

import (
	"time"

	"go.uber.org/zap"
)

type Option func(*SubscriptionHub)

func Withlogger(logger *zap.Logger) Option {
	return func(h *SubscriptionHub) {
		h.logger = logger.Named("hub")
	}
}

func WithSourceChannelSize(size int) Option {
	return func(h *SubscriptionHub) {
		h.sourceChannelSize = size
	}
}

func WithRealtimeTolerance(d time.Duration) Option {
	return func(h *SubscriptionHub) {
		h.realtimeTolerance = d
	}
}

func WithoutMemoization() Option {
	return func(h *SubscriptionHub) {
		h.skipMemoization = true
	}
}
