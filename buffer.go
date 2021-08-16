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
	"container/list"
	"fmt"
	"sync"

	"github.com/streamingfast/dmetrics"
	"go.uber.org/zap"
)

type Buffer struct {
	sync.RWMutex

	list     *list.List
	elements map[string]*list.Element // block ID to list Element

	countMetric *dmetrics.Gauge

	logger *zap.Logger
}

func NewBuffer(name string, logger *zap.Logger) *Buffer {
	return &Buffer{
		logger:   logger,
		list:     list.New(),
		elements: make(map[string]*list.Element),

		countMetric: Metrics.NewGauge(fmt.Sprintf("block_in_buffer_%s", name)),
	}
}

func (b *Buffer) PopTail() (blockRef BlockRef) {
	b.Lock()
	defer b.Unlock()

	elem := b.list.Front()
	if elem == nil {
		return nil
	}

	b.list.Remove(elem)
	blockRef = elem.Value.(BlockRef)
	delete(b.elements, blockRef.ID())

	b.countMetric.Dec()
	return blockRef
}

func (b *Buffer) Exists(id string) bool {
	return b.GetByID(id) != nil
}

func (b *Buffer) AppendHead(blk BlockRef) {
	id := blk.ID()

	b.Lock()
	defer b.Unlock()

	if _, found := b.elements[id]; found {
		b.logger.Debug("skipping block that was seen already in buffer map", zap.String("block_id", id), zap.Uint64("block_num", blk.Num()))
		return
	}

	el := b.list.PushBack(blk)
	b.elements[id] = el

	b.countMetric.Inc()
	return
}

func (b *Buffer) Head() (blk BlockRef) {
	b.RLock()
	defer b.RUnlock()

	elem := b.list.Back()
	if elem == nil {
		return nil
	}

	return elem.Value.(BlockRef)
}

// LastObject -> Head, same thing, better name.
// LastBlockInfo -> Head, same thing better name.

func (b *Buffer) Tail() (blk BlockRef) {
	b.RLock()
	defer b.RUnlock()

	elem := b.list.Front()
	if elem == nil {
		return nil
	}

	return elem.Value.(BlockRef)

}

// IterateAllObjects -> AllBlocks
// IterateObjects -> HeadBlocks

func (b *Buffer) AllBlocks() (out []BlockRef) {
	b.RLock()
	defer b.RUnlock()

	out = make([]BlockRef, b.list.Len())
	i := 0
	for elem := b.list.Front(); elem != nil; elem = elem.Next() {
		out[i] = elem.Value.(BlockRef)
		i++
	}
	return
}

func (b *Buffer) HeadBlocks(count int) []BlockRef {
	all := b.AllBlocks()
	if count >= len(all) {
		return all
	}
	return all[len(all)-count:]
}

// Len() locks the buffer and returns its length. Watch out for deadlocks between buffer.lock and promises.lock if using this internally.
func (b *Buffer) Len() int {
	b.RLock()
	defer b.RUnlock()

	return b.list.Len()
}

// GetBlockByID -> GetByID

func (b *Buffer) GetByID(id string) (blk BlockRef) {
	b.RLock()
	defer b.RUnlock()

	elem := b.elements[id]
	if elem == nil {
		return nil
	}
	return elem.Value.(BlockRef)
}

func (b *Buffer) Delete(blk BlockRef) {
	b.Lock()
	defer b.Unlock()

	elem := b.elements[blk.ID()]
	if elem != nil {
		b.list.Remove(elem)
	}
	delete(b.elements, blk.ID())

	b.countMetric.Dec()
}

func (b *Buffer) TruncateTail(lowBlockNumInclusive uint64) (truncated []BlockRef) {
	var remove []*list.Element

	b.Lock()
	defer b.Unlock()

	for elem := b.list.Front(); elem != nil; elem = elem.Next() {
		blk := elem.Value.(BlockRef)
		if blk.Num() <= lowBlockNumInclusive {
			truncated = append(truncated, blk)
			remove = append(remove, elem)
			delete(b.elements, blk.ID())
		}
	}

	for _, rem := range remove {
		b.list.Remove(rem)
		b.countMetric.Dec()
	}

	return truncated
}

func (b *Buffer) Contains(blockNum uint64) bool {
	b.RLock()
	defer b.RUnlock()

	for elem := b.list.Front(); elem != nil; elem = elem.Next() {
		if elem.Value.(BlockRef).Num() == blockNum {
			return true
		}
	}

	return false
}
