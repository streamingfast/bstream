package bstream

import (
	"fmt"
)

var GetBlockReaderFactory BlockReaderFactory
var GetBlockWriterFactory BlockWriterFactory
var GetBlockDecoder BlockDecoder
var GetBlockWriterHeaderLen int
var GetProtocolFirstStreamableBlock = uint64(0)
var GetProtocolGenesisBlock = uint64(0)

func ValidateRegistry() error {
	if GetBlockReaderFactory == nil {
		return fmt.Errorf("no block reader factory set, check that you set `bstream.GetBlockReaderFactory`")
	}

	if GetBlockDecoder == nil {
		return fmt.Errorf("no block decoder set, check that you set set `bstream.GetBlockDecoder`")
	}

	if GetBlockWriterFactory == nil {
		return fmt.Errorf("no block writer factory set, check that you set set `bstream.GetBlockWriterFactory`")
	}

	if GetBlockWriterHeaderLen == 0 {
		return fmt.Errorf("no block writer factory set, check that you set set `bstream.GetBlockWriterHeaderLen`")
	}

	return nil
}
