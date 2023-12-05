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
	message, err := l.src.ReadMessage()
	if len(message) > 0 {
		blk := new(pbbstream.Block)
		err = proto.Unmarshal(message, blk)
		if err != nil {
			return nil, fmt.Errorf("unable to read block proto: %s", err)
		}
		if err := supportLegacy(blk); err != nil {
			return nil, fmt.Errorf("support legacy block: %s", err)
		}

		return blk, nil
	}

	if err == io.EOF {
		return nil, err
	}

	// In all other cases, we are in an error path
	return nil, fmt.Errorf("failed reading next dbin message: %s", err)
}

func supportLegacy(b *pbbstream.Block) error {
	if b.Payload == nil {
		b.Payload = &anypb.Any{}
		switch b.PayloadKind {
		case pbbstream.Protocol_EOS:
			b.Payload.TypeUrl = "sf.antelope.type.v1.Block"
		case pbbstream.Protocol_ETH:
			b.Payload.TypeUrl = "sf.ethereum.type.v2.Block"
		case pbbstream.Protocol_COSMOS:
			b.Payload.TypeUrl = "sf.cosmos.type.v1.Block"
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
