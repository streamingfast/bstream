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
	"fmt"
	"sync"
	"time"
)

type SimpleTailManager struct {
	sync.Mutex

	bufferSize int
	buffer     *Buffer
	tailLock   *TailLock
}

func NewSimpleTailManager(buffer *Buffer, bufferSize int) *SimpleTailManager {
	return &SimpleTailManager{
		buffer:     buffer,
		bufferSize: bufferSize,
		tailLock:   NewTailLock(),
	}
}

func (m *SimpleTailManager) TailLock(blockNum uint64) (releaseFunc func(), err error) {
	m.Lock()
	defer m.Unlock()

	if !m.buffer.Contains(blockNum) {
		return nil, fmt.Errorf("buffer doesn't contain %d", blockNum)
	}

	return m.tailLock.TailLock(blockNum), nil
}

func (m *SimpleTailManager) Launch() {
	for {
		time.Sleep(3 * time.Second)
		m.attemptTruncation()
	}
}

func (m *SimpleTailManager) attemptTruncation() {
	if m.buffer.Len() < m.bufferSize {
		return
	}

	m.Lock()
	defer m.Unlock()

	blocks := m.buffer.AllBlocks()
	targetBlockNum := blocks[len(blocks)-m.bufferSize].Num()

	lower := m.tailLock.LowerBound()
	if lower != 0 && lower < targetBlockNum {
		targetBlockNum = lower
	}

	if blocks[0].Num() > targetBlockNum {
		return
	}

	_ = m.buffer.TruncateTail(targetBlockNum)
}
