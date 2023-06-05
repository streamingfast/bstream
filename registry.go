package bstream

import (
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/proto"
)

var GetBlockPayloadSetter BlockPayloadSetter
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
	if GetBlockPayloadSetter == nil {
		return fmt.Errorf(missingInitializationErrorMessage("GetBlockPayloadSetter"))
	}

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

// InitGeneric initializes `bstream` with a generic block payload setter, reader, decoder and writer that are suitable
// for all chains. This is used in `firehose-core` as well as in testing method in respective tests to instantiate
// bstream.
func InitGeneric(protocol string, protocolVersion int32, blockFactory func() proto.Message) {
	GetBlockWriterHeaderLen = 10
	GetMemoizeMaxAge = 20 * time.Second
	GetBlockPayloadSetter = MemoryBlockPayloadSetter

	GetBlockDecoder = BlockDecoderFunc(func(blk *Block) (any, error) {
		// blk.Kind() is not used anymore, only the content type and version is checked at read time now
		if blk.Version() != protocolVersion {
			return nil, fmt.Errorf("this decoder only knows about version %d, got %d", protocolVersion, blk.Version())
		}

		block := blockFactory()
		payload, err := blk.Payload.Get()
		if err != nil {
			return nil, fmt.Errorf("getting payload: %w", err)
		}

		err = proto.Unmarshal(payload, block)
		if err != nil {
			return nil, fmt.Errorf("unable to decode payload: %w", err)
		}

		return block, nil
	})

	GetBlockWriterFactory = BlockWriterFactoryFunc(func(writer io.Writer) (BlockWriter, error) {
		return NewDBinBlockWriter(writer, protocol, int(protocolVersion))
	})

	GetBlockReaderFactory = BlockReaderFactoryFunc(func(reader io.Reader) (BlockReader, error) {
		return NewDBinBlockReader(reader, func(contentType string, version int32) error {
			if contentType != protocol && version != protocolVersion {
				return fmt.Errorf("reader only knows about %s block kind at version %d, got %s at version %d", protocol, protocolVersion, contentType, version)
			}

			return nil
		})
	})
}
