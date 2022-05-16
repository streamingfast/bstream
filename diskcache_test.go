package bstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewMemoryBytesFunc(t *testing.T) {
	data := []byte{0x00, 0x01, 0x02}

	res, err := NewMemoryBytesFunc(data)
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func Test_DiskCache(t *testing.T) {
	c, err := NewDiskCache("/tmp/atm", "test", 1024, 1024)
	require.NoError(t, err)

	data := []byte{0x00, 0x01, 0x02}

	f, err := c.CacheBytesFunc(TestBlock("2345", "1234"), data)
	require.NoError(t, err)

	res, err := f()
	require.NoError(t, err)
	require.Equal(t, data, res)
}
