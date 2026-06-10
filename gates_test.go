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
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/stretchr/testify/assert"
)

func TestBlockTimestampGate(t *testing.T) {
	t0 := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2024, time.January, 1, 0, 0, 1, 0, time.UTC)
	t2 := time.Date(2024, time.January, 1, 0, 0, 2, 0, time.UTC)
	t3 := time.Date(2024, time.January, 1, 0, 0, 3, 0, time.UTC)

	tests := []struct {
		name          string
		gateTimestamp time.Time
		gateType      GateType
		blockTimes    []time.Time
		expectHandled []bool
	}{
		{
			name:          "inclusive gate passes block at exact timestamp",
			gateTimestamp: t2,
			gateType:      GateInclusive,
			blockTimes:    []time.Time{t0, t1, t2, t3},
			expectHandled: []bool{false, false, true, true},
		},
		{
			name:          "exclusive gate skips block at exact timestamp",
			gateTimestamp: t2,
			gateType:      GateExclusive,
			blockTimes:    []time.Time{t0, t1, t2, t3},
			expectHandled: []bool{false, false, false, true},
		},
		{
			name:          "inclusive gate passes all when gate timestamp is zero",
			gateTimestamp: time.Time{},
			gateType:      GateInclusive,
			blockTimes:    []time.Time{t0, t1},
			expectHandled: []bool{true, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handled int
			gate := NewBlockTimestampGate(tt.gateTimestamp, tt.gateType,
				HandlerFunc(func(_ *pbbstream.Block, _ any) error {
					handled++
					return nil
				}),
			)

			for i, ts := range tt.blockTimes {
				blk := TestBlockWithTimestamp("00000002a", "00000001a", ts)
				err := gate.ProcessBlock(blk, nil)
				assert.NoError(t, err)
				if tt.expectHandled[i] {
					assert.Equal(t, 1, handled, "block %d (ts=%s) should have been handled", i, ts)
				} else {
					assert.Equal(t, 0, handled, "block %d (ts=%s) should NOT have been handled", i, ts)
				}
				handled = 0
				if i > 0 && tt.expectHandled[i-1] {
					// gate already passed, reset for clarity; just verify it keeps flowing
					break
				}
			}
		})
	}
}

func TestRealtimeTripper(t *testing.T) {
	var tripped int
	var handled int

	trip := NewRealtimeTripper(1*time.Second,
		func() {
			tripped++
		},
		HandlerFunc(func(_ *pbbstream.Block, _ any) error {
			handled++
			return nil
		}),
	)

	trip.nowFunc = func() time.Time {
		return time.Date(2019, time.January, 1, 0, 0, 3, 0, time.UTC)
	}

	sec0 := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
	sec5 := time.Date(2019, time.January, 1, 0, 0, 5, 0, time.UTC)
	sec10 := time.Date(2019, time.January, 1, 0, 0, 10, 0, time.UTC)

	trip.ProcessBlock(TestBlockWithTimestamp("00000002a", "00000001a", sec0), nil)

	assert.Equal(t, 0, tripped)
	assert.Equal(t, 1, handled)

	trip.ProcessBlock(TestBlockWithTimestamp("00000002a", "00000001a", sec5), nil)

	assert.Equal(t, 1, tripped)
	assert.Equal(t, 2, handled)

	trip.ProcessBlock(TestBlockWithTimestamp("00000002a", "00000001a", sec10), nil)

	assert.Equal(t, 1, tripped)
	assert.Equal(t, 3, handled)
}
