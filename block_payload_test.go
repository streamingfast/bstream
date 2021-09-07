package bstream

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_MemoryBlockPayload_Get(t *testing.T) {
	GetBlockPayloadSetter = MemoryBlockPayloadSetter

	data := []byte{0x00, 0x01, 0x02}

	block := &Block{
		Id: "block.id.1",
	}

	var err error
	block, err = GetBlockPayloadSetter(block, data)
	require.NoError(t, err)

	payload, err := block.Payload.Get()
	require.NoError(t, err)
	require.Equal(t, data, payload)
}

func Test_FileBlockPayload_Get(t *testing.T) {
	GetBlockPayloadSetter = FileBlockPayloadSetter

	data := []byte{0x00, 0x01, 0x02}

	block := &Block{
		Id: "block.id.1",
	}

	var err error
	block, err = GetBlockPayloadSetter(block, data)
	require.NoError(t, err)

	payload, err := block.Payload.Get()
	require.NoError(t, err)
	require.Equal(t, data, payload)
}
