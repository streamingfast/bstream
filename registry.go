package bstream

// bstreams.NewDBinBlockReader
// var GetBlockReaderFactory BlockReaderFactory
// bstream.NewDBinBlockWriter
// var GetBlockWriterFactory BlockWriterFactory
// var GetBlockWriterHeaderLen int
var GetProtocolFirstStreamableBlock = uint64(0)
var GetMaxNormalLIBDistance = uint64(1000)
var NormalizeBlockID = func(in string) string { // some chains have block IDs that optionally start with 0x or are case insensitive
	return in
}

func ValidateRegistry() error {

	//if GetBlockReaderFactory == nil {
	//	return fmt.Errorf(missingInitializationErrorMessage("GetBlockReaderFactory"))
	//}
	//
	//if GetBlockWriterFactory == nil {
	//	return fmt.Errorf(missingInitializationErrorMessage("GetBlockWriterFactory"))
	//}
	//
	//if GetBlockWriterHeaderLen == 0 {
	//	return fmt.Errorf(missingInitializationErrorMessage("GetBlockWriterHeaderLen"))
	//}

	return nil
}

//func getBlockReaderFactory() BlockReaderFactory {
//	if GetBlockReaderFactory == nil {
//		panic(missingInitializationErrorMessage("GetBlockReaderFactory"))
//	}
//
//	return GetBlockReaderFactory
//}

//func missingInitializationErrorMessage(field string) string {
//	return fmt.Sprintf(`the global variable 'bstream.%s' is nil, it was not initialized correctly, you are probably missing a critical chain specific import that sets those value, usually in the form '_ "github.com/streamingfast/firehose-<chain>/types"' where '<chain>' is one of firehose supported chain`, field)
//}

// InitGeneric initializes `bstream` with a generic block payload setter, reader, decoder and writer that are suitable
// for all chains. This is used in `firehose-core` as well as in testing method in respective tests to instantiate
// bstream.
//func InitGeneric(protocol string) {
//	GetBlockWriterHeaderLen = dbin.HeaderLenght(protocol)
//
//	GetBlockWriterFactory = BlockWriterFactoryFunc(func(writer io.Writer) (BlockWriter, error) {
//		return NewDBinBlockWriter(writer, protocol)
//	})
//
//	GetBlockReaderFactory = BlockReaderFactoryFunc(func(reader io.Reader) (BlockReader, error) {
//		return NewDBinBlockReader(reader, func(contentType string) error {
//			if contentType != protocol {
//				return fmt.Errorf("reader only knows about %s block kind, got %s", protocol, contentType)
//			}
//
//			return nil
//		})
//	})
//}
