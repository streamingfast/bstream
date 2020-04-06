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

	"github.com/stretchr/testify/assert"
)

func TestTailLock(t *testing.T) {
	l := NewTailLock()

	release0 := l.TailLock(3)
	assert.EqualValues(t, 3, l.LowerBound(), "single lock did not set lowerbound")

	// should not crash
	release0()
	release0()

	assert.EqualValues(t, 0, l.LowerBound(), "single release did not zero LowerBound")

	release3 := l.TailLock(3)
	assert.EqualValues(t, 3, l.LowerBound(), "single lock did not set lowerbound")
	release4 := l.TailLock(4)
	assert.EqualValues(t, 3, l.LowerBound(), "higher lock changed lowerbound")
	release2 := l.TailLock(2)
	assert.EqualValues(t, 2, l.LowerBound(), "lower lock did not change lowerbound")

	release3()
	assert.EqualValues(t, 2, l.LowerBound(), "not lower release affected LowerBlock")

	release2()
	assert.EqualValues(t, 4, l.LowerBound(), "release lower block did not raise LowerBlock")

	release4()
	assert.EqualValues(t, 0, l.LowerBound(), "single release did not zero LowerBound")
}
