package forkable

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testUint64Metric struct {
	value uint64
	set   bool
}

func (t *testUint64Metric) SetUint64(value uint64) {
	t.value = value
	t.set = true
}

func TestForkable_FinalizedBlockNumMetric(t *testing.T) {
	finalized := &testUint64Metric{}

	p := New(&testForkableSink{}, WithFinalizedBlockNumMetric(finalized), WithExclusiveLIB(bRef("00000003a")))
	p.SetLiveMetrics()

	require.NoError(t, p.ProcessBlock(tb("00000004a", "00000003a", 3), nil))
	assert.Equal(t, uint64(3), finalized.value)

	require.NoError(t, p.ProcessBlock(tb("00000005a", "00000004a", 4), nil))
	assert.Equal(t, uint64(4), finalized.value)
}

func TestForkable_FinalizedBlockNumMetricNotSetWithoutLiveMetrics(t *testing.T) {
	finalized := &testUint64Metric{}

	p := New(&testForkableSink{}, WithFinalizedBlockNumMetric(finalized), WithExclusiveLIB(bRef("00000003a")))

	require.NoError(t, p.ProcessBlock(tb("00000004a", "00000003a", 3), nil))
	assert.False(t, finalized.set, "finalized block num metric must only be updated on the live path")
}
