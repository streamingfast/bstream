// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bstream

import (
	"fmt"
	"io"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/dbin"
	proto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// DBinBlockReader reads the dbin format where each element is assumed to be a `Block`.
type DBinBlockReader struct {
	src    *dbin.Reader
	Header *dbin.Header
}

func NewDBinBlockReader(reader io.Reader) (out *DBinBlockReader, err error) {
	return NewDBinBlockReaderWithValidation(reader, nil)
}

func NewDBinBlockReaderWithValidation(reader io.Reader, validateHeaderFunc func(contentType string) error) (out *DBinBlockReader, err error) {
	dbinReader := dbin.NewReader(reader)
	header, err := dbinReader.ReadHeader()
	if err != nil {
		return nil, fmt.Errorf("unable to read file header: %s", err)
	}

	if validateHeaderFunc != nil {
		err = validateHeaderFunc(header.ContentType)
		if err != nil {
			return nil, err
		}
	}

	return &DBinBlockReader{
		src:    dbinReader,
		Header: header,
	}, nil
}

func (l *DBinBlockReader) Read() (*pbbstream.Block, error) {
	return readMessage(l, func(message []byte) (*pbbstream.Block, error) {
		blk := new(pbbstream.Block)
		if err := proto.Unmarshal(message, blk); err != nil {
			return nil, fmt.Errorf("unable to read block proto: %s", err)
		}

		if err := supportLegacy(blk); err != nil {
			return nil, fmt.Errorf("support legacy block: %s", err)
		}

		return blk, nil
	})
}

// ReadAsBlockMeta reads the next message as a BlockMeta instead of as a Block leading
// to reduce memory constaint since the payload are "skipped". There is a memory pressure
// since we need to load the full block.
//
// But at least it's not persisent memory.
func (l *DBinBlockReader) ReadAsBlockMeta() (*pbbstream.BlockMeta, error) {
	return readMessage(l, func(message []byte) (*pbbstream.BlockMeta, error) {
		meta := new(pbbstream.BlockMeta)
		err := proto.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(message, meta)
		if err != nil {
			return nil, fmt.Errorf("unable to read block proto: %s", err)
		}
		if err := supportLegacyMeta(meta); err != nil {
			return nil, fmt.Errorf("support legacy block meta: %s", err)
		}

		return meta, nil
	})
}

func readMessage[T any](reader *DBinBlockReader, decoder func(message []byte) (T, error)) (out T, err error) {
	message, err := reader.src.ReadMessage()
	if len(message) > 0 {
		return decoder(message)
	}

	if err == io.EOF {
		return out, err
	}

	// In all other cases, we are in an error path
	return out, fmt.Errorf("failed reading next dbin message: %s", err)

}

func supportLegacy(b *pbbstream.Block) error {
	if b.Payload == nil {
		b.Payload = &anypb.Any{}
		switch b.PayloadKind {
		case pbbstream.Protocol_EOS:
			b.Payload.TypeUrl = "type.googleapis.com/sf.antelope.type.v1.Block"
		case pbbstream.Protocol_ETH:
			b.Payload.TypeUrl = "type.googleapis.com/sf.ethereum.type.v2.Block"
		case pbbstream.Protocol_COSMOS:
			b.Payload.TypeUrl = "type.googleapis.com/sf.cosmos.type.v1.Block"
		case pbbstream.Protocol_SOLANA:
			return fmt.Errorf("old block format from Solana protocol not supported, migrate your blocks")
		case pbbstream.Protocol_NEAR:
			return fmt.Errorf("old block format from NEAR protocol not supported, migrate your blocks")
		}
		b.Payload.Value = b.PayloadBuffer
		if b.Number > GetProtocolFirstStreamableBlock {
			b.ParentNum = b.Number - 1
		}
	}
	return nil
}

func supportLegacyMeta(b *pbbstream.BlockMeta) error {
	if b.ParentNum == 0 {
		// Boy, we cannot know with just parent num if it's a legacy block or not. This is because
		// the parent num could be legitimately 0, and we would not know if it's filled or not.
		// So, we use a hackish heuristic here, we check the block number, and if the difference
		// between the two is greater than 15, we assume parent number should have been filled.
		if b.Number > GetProtocolFirstStreamableBlock+15 {
			return fmt.Errorf("old block format without a properly populated parent num are not supported, migrate your blocks")
		}
	}

	return nil
}
