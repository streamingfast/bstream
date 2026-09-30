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
	"errors"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/streamingfast/dbin"
	"google.golang.org/protobuf/encoding/protowire"
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
		if err := unmarshalBlockAliasingPayload(message, blk); err != nil {
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

// Field numbers of sf.bstream.v1.Block and google.protobuf.Any read by
// unmarshalBlockAliasingPayload, they must stay in sync with proto/sf/bstream/v1/bstream.proto.
const (
	blockPayloadBufferField = 8
	blockPayloadField       = 11
	anyTypeURLField         = 1
	anyValueField           = 2
)

// unmarshalBlockAliasingPayload is proto.Unmarshal for a Block, except that the payload
// bytes (Payload.Value and the legacy PayloadBuffer) point into message instead of
// being copied out of it. The block's other fields are decoded by proto.Unmarshal.
//
// message must not be modified or reused afterwards, dbin.Reader.ReadMessage returns
// a new buffer for every message.
func unmarshalBlockAliasingPayload(message []byte, blk *pbbstream.Block) error {
	merge := proto.UnmarshalOptions{Merge: true}

	// The fields between two payload fields are decoded in one go, merging a message
	// decoded in parts is the same as decoding it whole.
	segmentStart := 0
	for pos := 0; pos < len(message); {
		num, typ, tagLen := protowire.ConsumeTag(message[pos:])
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(num, typ, message[pos+tagLen:])
		if valueLen < 0 {
			return protowire.ParseError(valueLen)
		}
		fieldEnd := pos + tagLen + valueLen

		if typ == protowire.BytesType && (num == blockPayloadField || num == blockPayloadBufferField) {
			if err := merge.Unmarshal(message[segmentStart:pos], blk); err != nil {
				return err
			}
			segmentStart = fieldEnd

			value, _ := protowire.ConsumeBytes(message[pos+tagLen:])
			if num == blockPayloadBufferField {
				blk.PayloadBuffer = value
			} else if err := mergeAnyAliasingValue(value, blk); err != nil {
				return err
			}
		}

		pos = fieldEnd
	}

	return merge.Unmarshal(message[segmentStart:], blk)
}

func mergeAnyAliasingValue(message []byte, blk *pbbstream.Block) error {
	if blk.Payload == nil {
		blk.Payload = &anypb.Any{}
	}

	for len(message) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(message)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}
		valueLen := protowire.ConsumeFieldValue(num, typ, message[tagLen:])
		if valueLen < 0 {
			return protowire.ParseError(valueLen)
		}

		if typ == protowire.BytesType && (num == anyTypeURLField || num == anyValueField) {
			value, _ := protowire.ConsumeBytes(message[tagLen:])
			if num == anyTypeURLField {
				if !utf8.Valid(value) {
					return errors.New("string field contains invalid UTF-8")
				}
				blk.Payload.TypeUrl = string(value)
			} else {
				blk.Payload.Value = value
			}
		}

		message = message[tagLen+valueLen:]
	}

	return nil
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
			_, solanaLegacy := os.LookupEnv("ACCEPT_SOLANA_LEGACY_BLOCK_FORMAT")
			_, legacy := os.LookupEnv("ACCEPT_LEGACY_BLOCK_FORMAT")

			if solanaLegacy || legacy {
				b.Payload.TypeUrl = "type.googleapis.com/sf.solana.type.v1.Block"
				break
			}
			return fmt.Errorf("old block format from Solana protocol not supported, migrate your blocks")
		case pbbstream.Protocol_NEAR:
			if _, ok := os.LookupEnv("ACCEPT_LEGACY_BLOCK_FORMAT"); ok {
				b.Payload.TypeUrl = "type.googleapis.com/sf.near.type.v1.Block"
				break
			}
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
