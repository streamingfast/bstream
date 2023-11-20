package pbbstream

import (
	"fmt"
	"time"
)

func (b *Block) Time() time.Time {
	if b == nil {
		return time.Time{}
	}
	if err := b.Timestamp.CheckValid(); err != nil {
		panic(fmt.Errorf("invalid timestamp: %w", err))
	}

	return b.Timestamp.AsTime()
}
func (b *Block) AsRef() BasicBlockRef {
	if b == nil {
		return BasicBlockRef{"", 0}
	}

	return BasicBlockRef{b.Id, b.Number}
}
func (b *Block) PreviousRef() *BasicBlockRef {
	if b == nil || b.ParentNum == 0 || b.ParentId == "" {
		return &BasicBlockRef{"", 0}
	}
	return &BasicBlockRef{b.ParentId, b.ParentNum}
}
func (b *Block) GetFirehoseBlockID() string           { return b.Id }
func (b *Block) GetFirehoseBlockNumber() uint64       { return b.Number }
func (b *Block) GetFirehoseBlockParentID() string     { return b.ParentId }
func (b *Block) GetFirehoseBlockParentNumber() uint64 { return b.ParentNum }
func (b *Block) GetFirehoseBlockTime() time.Time      { return b.Time() }

type BasicBlockRef struct {
	id  string
	num uint64
}

func (e BasicBlockRef) Num() uint64 { return e.num }
func (e BasicBlockRef) ID() string  { return e.id }
func (e BasicBlockRef) String() string {
	if e.id == "" && e.num == 0 {
		return "Block <nil>"
	}

	return fmt.Sprintf("#%d (%s)", e.num, e.id)
}
