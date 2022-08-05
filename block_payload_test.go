package bstream

// FIXME this was broken for a while
//func Test_MemoryBlockPayload_Get(t *testing.T) {
//	InitCache(".", "/tmp/atm", 1024, 1024)
//	GetBlockPayloadSetter = MemoryBlockPayloadSetter
//
//	data := []byte{0x00, 0x01, 0x02}
//
//	block := &Block{
//		Id: "block.id.1",
//	}
//
//	var err error
//	block, err = GetBlockPayloadSetter(block, data)
//	require.NoError(t, err)
//
//	payload, err := block.Payload.Get()
//	require.NoError(t, err)
//	require.Equal(t, data, payload)
//}
//
//func Test_DiskCachedBlockPayload_Get(t *testing.T) {
//	InitCache(".", "/tmp/atm", 1024, 1024)
//	GetBlockPayloadSetter = ATMCachedPayloadSetter
//
//	data := []byte{0x00, 0x01, 0x02}
//
//	block := &Block{
//		Id: "block.id.1",
//	}
//
//	var err error
//	block, err = GetBlockPayloadSetter(block, data)
//	require.NoError(t, err)
//
//	payload, err := block.Payload.Get()
//	require.NoError(t, err)
//	require.Equal(t, data, payload)
//}
