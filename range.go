package bstream

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap/zapcore"
)

var ErrOpenEndedRange = errors.New("open ended range")

// ParseRangeOption is the interface for options passed to ParseRange.
// Both RangeOptions (for configuring the Range) and parse configuration
// options (like WithDefaultStartBlock) implement this interface.
type ParseRangeOption interface {
	parseRangeOption()
}

// parseConfig holds configuration for parsing ranges
type parseConfig struct {
	defaultStartBlock *uint64
}

func (*parseConfig) parseRangeOption() {}

// WithDefaultStartBlock returns a parse option that sets the default start block
// used when the input has an empty or relative start value.
//
// Examples with WithDefaultStartBlock(5):
//   - ":+100"   → 5:105    (start defaults to 5, end is 5+100)
//   - "+10:+100" → 15:115  (start is 5+10, end is 15+100)
func WithDefaultStartBlock(block uint64) *parseConfig {
	return &parseConfig{defaultStartBlock: &block}
}

// ParseRange will parse a range with support for relative values.
//
// Supported formats:
//   - "5:10" or "5-10"     → explicit start and end
//   - "5:+100"             → explicit start, end is start + offset (5:105)
//   - ":+100" + option     → start from default, end is start + offset
//   - "+10:+100" + option  → start is default + offset, end is start + offset
//
// By default it will make an inclusive start & end, use RangeOptions to set exclusive boundaries.
//
// For inputs with empty or relative start values (e.g., ":+100" or "+10:+100"),
// you must provide WithDefaultStartBlock option, otherwise an error is returned.
func ParseRange(in string, opts ...ParseRangeOption) (*Range, error) {
	if in == "" {
		return nil, fmt.Errorf("input is required")
	}

	// Separate parse options from range options
	var cfg parseConfig
	var rangeOpts []RangeOptions
	for _, opt := range opts {
		switch o := opt.(type) {
		case *parseConfig:
			if o.defaultStartBlock != nil {
				cfg.defaultStartBlock = o.defaultStartBlock
			}
		case RangeOptions:
			rangeOpts = append(rangeOpts, o)
		}
	}

	// Split by : or - but preserve the parts with + prefix
	parts := splitRangeParts(in)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range format: expected 'start:end' or 'start-end', got %q", in)
	}

	startPart := strings.TrimSpace(parts[0])
	endPart := strings.TrimSpace(parts[1])

	var startBlock uint64
	var endBlock uint64

	// Parse start block
	if startPart == "" {
		// Empty start, use default
		if cfg.defaultStartBlock == nil {
			return nil, fmt.Errorf("empty start block requires WithDefaultStartBlock option")
		}
		startBlock = *cfg.defaultStartBlock
	} else if strings.HasPrefix(startPart, "+") {
		// Relative start
		if cfg.defaultStartBlock == nil {
			return nil, fmt.Errorf("relative start block %q requires WithDefaultStartBlock option", startPart)
		}
		offset, err := parseBlockNumber(strings.TrimPrefix(startPart, "+"))
		if err != nil {
			return nil, fmt.Errorf("invalid start block offset: %w", err)
		}
		startBlock = *cfg.defaultStartBlock + offset
	} else {
		// Absolute start
		val, err := parseBlockNumber(startPart)
		if err != nil {
			return nil, fmt.Errorf("invalid start block: %w", err)
		}
		startBlock = val
	}

	// Parse end block
	if strings.HasPrefix(endPart, "+") {
		// Relative end (relative to start)
		offset, err := parseBlockNumber(strings.TrimPrefix(endPart, "+"))
		if err != nil {
			return nil, fmt.Errorf("invalid end block offset: %w", err)
		}
		endBlock = startBlock + offset
	} else {
		// Absolute end
		val, err := parseBlockNumber(endPart)
		if err != nil {
			return nil, fmt.Errorf("invalid stop block: %w", err)
		}
		endBlock = val
	}

	r, err := newRange(startBlock, &endBlock, rangeOpts...)
	if err != nil {
		return nil, fmt.Errorf("making range: %w", err)
	}

	return r, nil
}

// splitRangeParts splits the input by : or - while preserving + prefixes
func splitRangeParts(in string) []string {
	// Find the separator (: or -)
	for i, r := range in {
		if r == ':' || r == '-' {
			// Check it's not within a number (e.g., not the minus in a negative number)
			// For our purposes, : and - are always separators
			return []string{in[:i], in[i+1:]}
		}
	}
	return []string{in}
}

// parseBlockNumber parses a block number, stripping commas, underscores and spaces
func parseBlockNumber(s string) (uint64, error) {
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.ReplaceAll(s, "_", "")
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func MustParseRange(in string, opts ...ParseRangeOption) *Range {
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

func (RangeOptions) parseRangeOption() {}

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
		return "[nil]"
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
	if r.startBlock != other.startBlock ||
		r.exclusiveStartBlock != other.exclusiveStartBlock ||
		r.exclusiveEndBlock != other.exclusiveEndBlock {
		return false
	}
	if r.endBlock == nil || other.endBlock == nil {
		return r.endBlock == nil && other.endBlock == nil
	}
	return *r.endBlock == *other.endBlock
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
