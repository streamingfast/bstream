package hub

import (
	"testing"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/assert"
)

func TestSubstractAndRoundDownBlocks(t *testing.T) {
	prev := bstream.GetProtocolFirstStreamableBlock
	defer func() { bstream.GetProtocolFirstStreamableBlock = prev }()
	bstream.GetProtocolFirstStreamableBlock = 0

	tests := []struct {
		name       string
		blknum     uint64
		sub        uint64
		bundleSize uint64
		expect     uint64
	}{
		{"round to 100", 12345, 100, 100, 12200},
		{"exact boundary 100", 12400, 100, 100, 12300},
		{"underflow clamps to 0", 50, 100, 100, 0},
		{"round to 1000", 12345, 100, 1000, 12000},
		{"exact boundary 1000", 13000, 1000, 1000, 12000},
		{"underflow clamps to 0 at 1000", 500, 1000, 1000, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expect, substractAndRoundDownBlocks(test.blknum, test.sub, test.bundleSize))
		})
	}

	bstream.GetProtocolFirstStreamableBlock = 2
	assert.Equal(t, uint64(2), substractAndRoundDownBlocks(50, 100, 100), "clamps to first streamable block")
}
