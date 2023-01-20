package bstream

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRange_NewRangeContaining(t *testing.T) {
	tests := []struct {
		name        string
		blockNum    uint64
		size        uint64
		expectRange *Range
		expectError bool
	}{
		{"middle of range", 10, 20, &Range{0, ptr(20), false, false}, false},
		{"start of range", 10, 10, &Range{10, ptr(20), false, false}, false},
		{"end of range of range", 60, 20, &Range{60, ptr(80), false, false}, false},
		{"start of range", 0, 20, &Range{0, ptr(20), false, false}, false},
		{"no range size", 10, 0, &Range{0, ptr(20), false, false}, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := NewRangeContaining(test.blockNum, test.size)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectRange, v)
			}

		})
	}
}

func TestRange_Contains(t *testing.T) {
	tests := []struct {
		name     string
		blkRange *Range
		blockNum uint64
		expect   bool
	}{
		{"inclusive range, containing block num", &Range{10, ptr(15), false, false}, 12, true},
		{"inclusive range, block num less than start block", &Range{10, ptr(15), false, false}, 8, false},
		{"inclusive range, block num equal start block", &Range{10, ptr(15), false, false}, 10, true},
		{"inclusive range, block num greater than end block", &Range{10, ptr(15), false, false}, 17, false},
		{"inclusive range, block num equal end block", &Range{10, ptr(15), false, false}, 15, true},

		{"exclusive range, containing block num", &Range{10, ptr(15), true, true}, 12, true},
		{"exclusive range, block num less than start block", &Range{10, ptr(15), true, true}, 8, false},
		{"exclusive range, block num equal start block", &Range{10, ptr(15), true, true}, 10, false},
		{"exclusive range, block num greater than end block", &Range{10, ptr(15), true, true}, 17, false},
		{"exclusive range, block num equal end block", &Range{10, ptr(15), true, true}, 15, false},

		{"inclusive start, exclusive end, containing block num", &Range{10, ptr(15), false, true}, 12, true},
		{"inclusive start, exclusive end, block num less than start block", &Range{10, ptr(15), false, true}, 8, false},
		{"inclusive start, exclusive end, block num equal start block", &Range{10, ptr(15), false, true}, 10, true},
		{"inclusive start, exclusive end, block num greater than end block", &Range{10, ptr(15), false, true}, 17, false},
		{"inclusive start, exclusive end, block num equal end block", &Range{10, ptr(15), false, true}, 15, false},

		{"inclusive start, open ended, containing block num", &Range{10, nil, false, true}, 12, true},
		{"inclusive start, open ended, block num less than start block", &Range{10, nil, false, true}, 8, false},
		{"inclusive start, open ended, block num equal start block", &Range{10, nil, false, true}, 10, true},
		{"inclusive start, open ended, block num greater than end block", &Range{10, nil, false, true}, 17, true},
		{"inclusive start, open ended, block num equal end block", &Range{10, nil, false, true}, 15, true},

		{"exclusive start, open ended, containing block num", &Range{10, nil, true, false}, 12, true},
		{"exclusive start, open ended, block num less than start block", &Range{10, nil, true, false}, 8, false},
		{"exclusive start, open ended, block num equal start block", &Range{10, nil, true, false}, 10, false},
		{"exclusive start, open ended, block num greater than end block", &Range{10, ptr(15), true, false}, 17, false},
		{"exclusive start, open ended, block num equal end block", &Range{10, nil, true, false}, 15, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, test.blkRange.Contains(test.blockNum))
		})
	}
}

func TestRange_String(t *testing.T) {
	tests := []struct {
		name     string
		blkRange *Range
		expect   string
	}{
		{"inclusive range", &Range{10, ptr(15), false, false}, "[10, 15]"},
		{"exclusive range", &Range{10, ptr(15), true, true}, "(10, 15)"},
		{"inclusive start, exclusive end", &Range{10, ptr(15), false, true}, "[10, 15)"},
		{"exclusive start, inclusive end", &Range{10, ptr(15), true, false}, "(10, 15]"},
		{"inclusive start, open ended", &Range{10, nil, false, false}, "[10, nil]"},
		{"exclusive start, open ended", &Range{10, nil, true, false}, "(10, nil]"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, test.blkRange.String())
		})
	}
}

func TestRange_ReachedEndBlock(t *testing.T) {
	tests := []struct {
		name     string
		blkRange *Range
		blockNum uint64
		expect   bool
	}{
		{"inclusive end block, less then end block", &Range{10, ptr(15), false, false}, 14, false},
		{"inclusive end block, at end block", &Range{10, ptr(15), false, false}, 15, true},
		{"inclusive end block, greater then end block", &Range{10, ptr(15), false, false}, 16, true},

		{"exclusive end block, less then end block", &Range{10, ptr(15), false, true}, 14, true},
		{"exclusive end block, at end block", &Range{10, ptr(15), false, true}, 15, true},
		{"exclusive end block, greater then end block", &Range{10, ptr(15), false, true}, 16, true},

		{"open ended, less then end block", &Range{10, nil, false, true}, 14, false},
		{"open ended, at end block", &Range{10, nil, false, true}, 15, false},
		{"open ended, greater then end block", &Range{10, nil, false, true}, 16, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, test.blkRange.ReachedEndBlock(test.blockNum))
		})
	}
}

func TestRange_Next(t *testing.T) {
	tests := []struct {
		name     string
		blkRange *Range
		expect   *Range
	}{
		{"inclusive range", &Range{10, ptr(15), false, false}, &Range{15, ptr(20), false, false}},
		{"exclusive range", &Range{10, ptr(15), true, true}, &Range{15, ptr(20), true, true}},
		{"inclusive start, exclusive end", &Range{10, ptr(15), false, true}, &Range{15, ptr(20), false, true}},
		{"exclusive start, inclusive end", &Range{10, ptr(15), true, false}, &Range{15, ptr(20), true, false}},
		{"inclusive start, open ended", &Range{10, nil, false, false}, &Range{15, nil, false, false}},
		{"exclusive start, open ended", &Range{10, nil, true, false}, &Range{15, nil, true, false}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, test.blkRange.Next(5))
		})
	}
}

func TestRange_Previous(t *testing.T) {
	tests := []struct {
		name     string
		blkRange *Range
		expect   *Range
	}{
		{"inclusive range", &Range{10, ptr(15), false, false}, &Range{5, ptr(10), false, false}},
		{"exclusive range", &Range{10, ptr(15), true, true}, &Range{5, ptr(10), true, true}},
		{"inclusive start, exclusive end", &Range{10, ptr(15), false, true}, &Range{5, ptr(10), false, true}},
		{"exclusive start, inclusive end", &Range{10, ptr(15), true, false}, &Range{5, ptr(10), true, false}},
		{"inclusive start, open ended", &Range{10, nil, false, false}, &Range{5, nil, false, false}},
		{"exclusive start, open ended", &Range{10, nil, true, false}, &Range{5, nil, true, false}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, test.blkRange.Previous(5))
		})
	}
}
func TestRange_ParseRange(t *testing.T) {
	tests := []struct {
		name        string
		stringRange string
		expectStart uint64
		expectEnd   uint64
		expectedErr require.ErrorAssertionFunc
		validRange  bool
	}{
		{"dash separator, no spaces", "20-40", 20, 40, require.NoError, true},
		{"colon separator, no spaces", "15:50", 15, 50, require.NoError, true},
		{"dash separator, with spaces", "30 - 70", 30, 70, require.NoError, true},
		{"colon separator, with spaces", "10 : 20", 10, 20, require.NoError, true},
		{"dash separator, with spaces, comma'd", "1,000 - 9,000", 1000, 9000, require.NoError, true},
		{"colon separator, with spaces, comma'd", "54,000 : 1,000,000", 54000, 1000000, require.NoError, true},
		{"colon separator, invalid range", "100 : 50", 0, 0, errorEqual("making range: invalid block range start 100, end 50"), false},
		{"dash separator, invalid range", "2,000,000-2,000,000", 0, 0, errorEqual("making range: invalid block range start 2000000, end 2000000"), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testRange, err := ParseRange(test.stringRange)
			if test.validRange {
				assert.Equal(t, test.expectStart, testRange.startBlock)
				assert.Equal(t, test.expectEnd, *testRange.endBlock)
			}
			test.expectedErr(t, err)
		})
	}
}
func TestRange_Size(t *testing.T) {
	tests := []struct {
		name        string
		blkRange    *Range
		expect      uint64
		expectError bool
	}{
		{"inclusive range", &Range{10, ptr(15), false, false}, 5, false},
		{"exclusive range", &Range{10, ptr(15), true, true}, 5, false},
		{"inclusive start, exclusive end", &Range{10, ptr(15), false, true}, 5, false},
		{"exclusive start, inclusive end", &Range{10, ptr(15), true, false}, 5, false},
		{"inclusive start, open ended", &Range{10, nil, false, false}, 0, true},
		{"exclusive start, open ended", &Range{10, nil, true, false}, 0, true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := test.blkRange.Size()
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expect, v)
			}

		})
	}
}

func TestRange_Split(t *testing.T) {
	tests := []struct {
		name        string
		blkRange    *Range
		expect      []*Range
		expectError bool
	}{
		{
			name:     "inclusive range",
			blkRange: &Range{10, ptr(25), false, false},
			expect: []*Range{
				{10, ptr(15), false, false},
				{15, ptr(20), false, false},
				{20, ptr(25), false, false},
			},
			expectError: false,
		},
		{
			name:     "exclusive range",
			blkRange: &Range{10, ptr(15), true, true},
			expect: []*Range{
				{10, ptr(15), true, true},
			},
			expectError: false,
		},
		{
			name:     "inclusive start, exclusive end",
			blkRange: &Range{10, ptr(12), false, true},
			expect: []*Range{
				{10, ptr(12), false, true},
			},
			expectError: false,
		},
		{
			name:     "exclusive start, inclusive end",
			blkRange: &Range{10, ptr(28), true, false},
			expect: []*Range{
				{10, ptr(15), true, false},
				{15, ptr(20), true, false},
				{20, ptr(25), true, false},
				{25, ptr(28), true, false},
			},
			expectError: false,
		},
		{
			name:        "inclusive start, open ended",
			blkRange:    &Range{10, nil, false, false},
			expectError: true,
		},
		{
			name:        "exclusive start, open ended",
			blkRange:    &Range{10, nil, true, false},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v, err := test.blkRange.Split(5)
			if test.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expect, v)
			}

		})
	}
}

func errorEqual(expectedErrString string) require.ErrorAssertionFunc {
	return func(t require.TestingT, err error, msgAndArgs ...interface{}) {
		require.EqualError(t, err, expectedErrString, msgAndArgs...)
	}
}
