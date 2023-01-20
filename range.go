package bstream

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"go.uber.org/zap/zapcore"
)

var ErrOpenEndedRange = errors.New("open ended range")

// ParseRange will parse a range of format 5-10, by default it will make an inclusive start & end
// use options to set exclusive boundaries
func ParseRange(in string, opts ...RangeOptions) (*Range, error) {
	if in == "" {
		return nil, fmt.Errorf("input is required")
	}
	ch := strings.FieldsFunc(in, splitBy)
	for i, bound := range ch {
		bound = strings.ReplaceAll(bound, " ", "")
		bound = regexp.MustCompile(`[^a-zA-Z0-9 ]+`).ReplaceAllString(bound, "")
		ch[i] = bound
	}
	lo, err := strconv.ParseInt(ch[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid start block: %w", err)
	}
	hi, err := strconv.ParseInt(ch[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid stop block: %w", err)
	}
	v := uint64(hi)

	r, err := newRange(uint64(lo), &v, opts...)
	if err != nil {
		return nil, fmt.Errorf("making range: %w", err)
	}

	return r, nil
}

func splitBy(r rune) bool {
	return r == ':' || r == '-'
}

func MustParseRange(in string, opts ...RangeOptions) *Range {
	r, err := ParseRange(in, opts...)
	if err != nil {
		panic(err)
	}
	return r
}

func NewRangeContaining(blockNum uint64, size uint64) (*Range, error) {
	if size == 0 {
		return nil, fmt.Errorf("range needs a size")
	}
	start := blockNum - (blockNum % size)
	return NewInclusiveRange(start, start+size), nil
}

type Range struct {
	startBlock          uint64
	endBlock            *uint64
	exclusiveStartBlock bool
	exclusiveEndBlock   bool
}

type RangeOptions func(p *Range) *Range

func WithExclusiveEnd() RangeOptions {
	return func(p *Range) *Range {
		p.exclusiveEndBlock = true
		return p
	}
}

func WithExclusiveStart() RangeOptions {
	return func(p *Range) *Range {
		p.exclusiveStartBlock = true
		return p
	}
}

func NewOpenRange(startBlock uint64) *Range {
	return mustNewRange(startBlock, nil, WithExclusiveEnd())
}

func NewRangeExcludingEnd(startBlock, endBlock uint64) *Range {
	return mustNewRange(startBlock, &endBlock, WithExclusiveEnd())
}

func NewInclusiveRange(startBlock, endBlock uint64) *Range {
	return mustNewRange(startBlock, &endBlock)
}

// mustNewRange return a new range, by default it will make an inclusive start & end
// use options to set exclusive boundaries
func mustNewRange(startBlock uint64, endBlock *uint64, opts ...RangeOptions) *Range {
	r, err := newRange(startBlock, endBlock, opts...)
	if err != nil {
		panic(err)
	}
	return r
}

// newRange return a new range, by default it will make an inclusive start & end
// use options to set exclusive boundaries
func newRange(startBlock uint64, endBlock *uint64, opts ...RangeOptions) (*Range, error) {
	if endBlock != nil && *endBlock <= startBlock {
		return nil, fmt.Errorf("invalid block range start %d, end %d", startBlock, *endBlock)
	}
	r := &Range{startBlock, endBlock, false, false}
	for _, opt := range opts {
		r = opt(r)
	}
	return r, nil
}

func (r *Range) StartBlock() uint64 { return r.startBlock }
func (r *Range) EndBlock() *uint64  { return r.endBlock }
func (r *Range) String() string {
	if r == nil {
		return fmt.Sprintf("[nil]")
	}
	startBlockDeli := "["
	if r.exclusiveStartBlock {
		startBlockDeli = "("
	}
	if r.endBlock == nil {
		return fmt.Sprintf("%s%d, nil]", startBlockDeli, r.startBlock)
	}
	endBlockDeli := "]"
	if r.exclusiveEndBlock {
		endBlockDeli = ")"
	}
	return fmt.Sprintf("%s%d, %d%s", startBlockDeli, r.startBlock, *r.endBlock, endBlockDeli)
}

func (r *Range) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if r == nil {
		enc.AddBool("nil", true)
	} else {
		if r.exclusiveStartBlock {
			enc.AddUint64("exclusive_start_block", r.startBlock)
		} else {
			enc.AddUint64("start_block", r.startBlock)
		}

		if r.endBlock == nil {
			enc.AddString("end_block", "None")
		} else {
			if r.exclusiveEndBlock {
				enc.AddUint64("exclusive_end_block", *r.endBlock)
			} else {
				enc.AddUint64("end_block", *r.endBlock)
			}
		}

	}
	return nil
}

func (r *Range) Contains(blockNum uint64) bool {
	if blockNum < r.startBlock {
		return false
	}
	if r.exclusiveStartBlock && blockNum == r.startBlock {
		return false
	}
	if r.endBlock == nil {
		return true
	}
	endBlock := *r.endBlock
	if blockNum > endBlock {
		return false
	}
	if r.exclusiveEndBlock && blockNum == endBlock {
		return false
	}
	return true
}

// block Number = 5

func (r *Range) ReachedEndBlock(blockNum uint64) bool {
	if r.endBlock == nil {
		return false
	}
	endBlock := *r.endBlock
	if blockNum >= endBlock {
		return true
	}
	if r.exclusiveEndBlock && blockNum == (endBlock-1) {
		return true
	}
	return false
}

func (r *Range) Next(size uint64) *Range {
	nextRange := &Range{
		exclusiveEndBlock:   r.exclusiveEndBlock,
		exclusiveStartBlock: r.exclusiveStartBlock,
	}
	if r.endBlock == nil {
		nextRange.startBlock = r.startBlock + size
		return nextRange
	}
	nextRange.startBlock = *r.endBlock
	endBlock := (*r.endBlock + size)
	nextRange.endBlock = &endBlock
	return nextRange
}

func (r *Range) Previous(size uint64) *Range {
	prevRange := &Range{
		startBlock:          r.startBlock - size,
		exclusiveEndBlock:   r.exclusiveEndBlock,
		exclusiveStartBlock: r.exclusiveStartBlock,
	}
	if r.endBlock == nil {
		return prevRange
	}
	prevRange.endBlock = &r.startBlock
	return prevRange
}

func (r *Range) IsNext(next *Range, size uint64) bool {
	return r.Next(size).Equals(next)
}

func (r *Range) Equals(other *Range) bool {
	return r.startBlock == other.startBlock &&
		r.endBlock == other.endBlock &&
		r.exclusiveStartBlock == other.exclusiveStartBlock &&
		r.exclusiveEndBlock == other.exclusiveEndBlock
}

func (r *Range) Size() (uint64, error) {
	if r.endBlock == nil {
		return 0, ErrOpenEndedRange
	}
	return *r.endBlock - r.startBlock, nil
}

func (r *Range) Split(chunkSize uint64) ([]*Range, error) {
	if r.endBlock == nil {
		return nil, ErrOpenEndedRange
	}

	endBlock := *r.endBlock

	if endBlock-r.startBlock <= chunkSize {
		return []*Range{r}, nil
	}

	var res []*Range
	currentEnd := (r.startBlock + chunkSize) - (r.startBlock+chunkSize)%chunkSize
	currentStart := r.startBlock

	for {
		res = append(res, &Range{
			startBlock:          currentStart,
			endBlock:            ptr(currentEnd),
			exclusiveStartBlock: r.exclusiveStartBlock,
			exclusiveEndBlock:   r.exclusiveEndBlock,
		})

		if currentEnd >= endBlock {
			break
		}

		currentStart = currentEnd
		currentEnd = currentStart + chunkSize
		if currentEnd > endBlock {
			currentEnd = endBlock
		}
	}

	return res, nil
}

func ptr(v uint64) *uint64 {
	return &v
}
