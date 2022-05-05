package bstream

type ChainConfig struct {
	BlockReaderFactory                 BlockReaderFactory
	BlockWriterFactory                 BlockWriterFactory
	BlockPayloadSetter                 BlockPayloadSetter
	BlockDecoder                       BlockDecoder
	FirstStreamableBlock               uint64
	MaxNormalIrreversibleBlockDistance uint64
}
