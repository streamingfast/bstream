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

	"github.com/dfuse-io/dmetrics"
	"go.uber.org/zap"
)

type Buffer struct {
	sync.RWMutex

	list     *list.List
	elements map[string]*list.Element // block ID to list Element
	name     string

	countMetric *dmetrics.Gauge
}

func NewBuffer(name string) *Buffer {
	return &Buffer{
		name:     name,
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
		zlog.Debug("skipping block that was seen already in buffer map", zap.String("buffer_name", b.name), zap.String("block_id", id), zap.Uint64("block_num", blk.Num()))
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

// func (b *BlockBuffer) SetPreventsRemovalFunc(f func(BlockRef, BlockRef) bool) {
// 	b.preventsRemovalFunc = f
// }

// func (b *BlockBuffer) SetOnDeleteFunc(f func(interface{})) {
// 	b.onDeleteFunc = f
// }

// truncate assumes caller locked buffer before invoking us
// func (b *Buffer) truncate() {
// 	if b.list.Len() < b.capacity {
// 		return
// 	}

// 	nextBoundary := nextBlockNumBoundary(b.list.Front().Value.(*objectWithBlockInfo))
// 	var toDelete []*list.Element
// 	elem := b.list.Front() // tail of chain in buffer
// 	for {
// 		if elem == nil {
// 			break
// 		}

// 		obj := elem.Value.(*objectWithBlockInfo)
// 		if obj.num >= nextBoundary {
// 			if b.preventsRemovalFunc(nextBoundary-1) || b.promises.preventsRemove(nextBoundary-1) {
// 				if b.capacity+1000 <= b.list.Len() {
// 					var proms []uint64
// 					for key := range b.promises.proms {
// 						proms = append(proms, key)
// 					}
// 					zlog.Debug("truncate prevented past 1000", zap.Uint64s("proms", proms))
// 				}
// 				return
// 			}

// 			zlog.Debug("truncating buffer up to next boundary", zap.Uint64("next_boundary", nextBoundary), zap.String("buffer_name", b.name))
// 			for _, x := range toDelete {
// 				deleting := x.Value.(*objectWithBlockInfo)
// 				if b.onDeleteFunc != nil {
// 					b.onDeleteFunc(deleting.object)
// 				}

// 				_ = b.list.Remove(x)
// 				id := deleting.id
// 				zlog.Debug("deleting element", zap.String("block_id", id), zap.Uint64("block_num", deleting.num))
// 				delete(b.elements, id)
// 			}

// 			break
// 		}

// 		toDelete = append(toDelete, elem)
// 		elem = elem.Next()
// 	}
// }

// func nextBlockNumBoundary(element *objectWithBlockInfo) uint64 {
// 	return (element.num + 100) / 100 * 100
// }

// func (g *TailLock) preventsRemove(blockNum uint64) bool {
// 	g.Lock()
// 	defer g.Unlock()
// 	for bnum := range g.inflights {
// 		if bnum < blockNum {
// 			return true
// 		}
// 	}

// 	g.lowerBound = blockNum // also serves initialization of that value, allowing further promises...
// 	return false
// }
