package bstream

import (
	"fmt"
)

var GetBlockReaderFactory BlockReaderFactory
var GetBlockWriterFactory BlockWriterFactory
var GetBlockDecoder BlockDecoder
var GetBlockWriterHeaderLen int
var GetProtocolFirstStreamableBlock = uint64(0)
var GetMaxNormalLIBDistance = uint64(1000)
var NormalizeBlockID = func(in string) string { // some chains have block IDs that optionally start with 0x or are case insensitive
	return in
}

func ValidateRegistry() error {
	if GetBlockReaderFactory == nil {
		return fmt.Errorf(missingInitializationErrorMessage("GetBlockReaderFactory"))
	}

	if GetBlockDecoder == nil {
		return fmt.Errorf(missingInitializationErrorMessage("GetBlockDecoder"))
	}

	if GetBlockWriterFactory == nil {
		return fmt.Errorf(missingInitializationErrorMessage("GetBlockWriterFactory"))
	}

	if GetBlockWriterHeaderLen == 0 {
		return fmt.Errorf(missingInitializationErrorMessage("GetBlockWriterHeaderLen"))
	}

	return nil
}

func getBlockReaderFactory() BlockReaderFactory {
	if GetBlockReaderFactory == nil {
		panic(missingInitializationErrorMessage("GetBlockReaderFactory"))
	}

	return GetBlockReaderFactory
}

func getBlockDecoder() BlockDecoder {
	if GetBlockDecoder == nil {
		panic(missingInitializationErrorMessage("GetBlockDecoder"))
	}

	return GetBlockDecoder
}

func missingInitializationErrorMessage(field string) string {
	return fmt.Sprintf(`the global variable 'bstream.%s' is nil, it was not initialized correctly, you are probably missing a critical chain specific import that sets those value, usually in the form '_ "github.com/streamingfast/firehose-<chain>/types"' where '<chain>' is one of firehose supported chain`, field)
}
