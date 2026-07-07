package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithMergedBlocksBundleSize(t *testing.T) {
	s := &Stream{}
	WithMergedBlocksBundleSize(1000)(s)
	assert.Equal(t, uint64(1000), s.mergedBlocksBundleSize)
}
