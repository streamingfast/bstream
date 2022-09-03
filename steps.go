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
	"strings"
)

type StepType int

const (
	StepNew  = StepType(1) //  First time we're seeing this block
	StepUndo = StepType(2) // We are undoing this block (it was sent as New previously)

	// (deprecated values for 4, 8)

	StepFinal = StepType(16) // This block is now final and cannot be 'Undone' anymore (irreversible)

	StepStale    = StepType(32)                                         // This block passed the LIB and is definitely forked out
	StepNewFinal = StepType(StepNew | StepFinal)                        //5 First time we're seeing this block, but we already know that it is irreversible
	StepsAll     = StepType(StepNew | StepUndo | StepFinal | StepStale) //7 useful for filters
)

const (
	StepNewString   = "new"
	StepUndoString  = "undo"
	StepFinalString = "final"
	StepStaleString = "stale"
)

func (t StepType) Matches(other StepType) bool {
	return t&other != 0
}

func (t StepType) String() string {
	var el []string
	if t.Matches(StepNew) {
		el = append(el, StepNewString)
	}
	if t.Matches(StepUndo) {
		el = append(el, StepUndoString)
	}
	if t.Matches(StepFinal) {
		el = append(el, StepFinalString)
	}
	if t.Matches(StepStale) {
		el = append(el, StepStaleString)
	}
	if len(el) == 0 {
		return "none"
	}
	return strings.Join(el, ",")
}
