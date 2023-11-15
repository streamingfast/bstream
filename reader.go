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

	"github.com/streamingfast/dbin"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	proto "google.golang.org/protobuf/proto"
)

// BlockReader is a reader protocol reading out bstream `Block` from a
// stream source. The reader respects the `io.Reader` contract in
// respect to `io.EOF`, i.e. it's possible to that both `block, io.EOF`
// be returned by the reader.
//
// You shall treat a non-nil block regardless of the `err` as if present,
// it's guaranteed it's valid. The subsequent call will still return `nil, io.EOF`.
type BlockReader interface {
	Read() (*Block, error)
}

type BlockReaderFactory interface {
	New(reader io.Reader) (BlockReader, error)
}

type BlockReaderFactoryFunc func(reader io.Reader) (BlockReader, error)

func (f BlockReaderFactoryFunc) New(reader io.Reader) (BlockReader, error) {
	return f(reader)
}

var _ BlockReader = (*DBinBlockReader)(nil)

// DBinBlockReader reads the dbin format where each element is assumed to be a `Block`.
type DBinBlockReader struct {
	src *dbin.Reader
}

func NewDBinBlockReader(reader io.Reader, validateHeaderFunc func(contentType string) error) (out *DBinBlockReader, err error) {
	dbinReader := dbin.NewReader(reader)
	contentType, err := dbinReader.ReadHeader()
	if err != nil {
		return nil, fmt.Errorf("unable to read file header: %s", err)
	}

	if validateHeaderFunc != nil {
		err = validateHeaderFunc(contentType)
		if err != nil {
			return nil, err
		}
	}

	return &DBinBlockReader{
		src: dbinReader,
	}, nil
}

func (l *DBinBlockReader) Read() (*Block, error) {
	message, err := l.src.ReadMessage()
	if len(message) > 0 {
		pbBlock := new(pbbstream.Block)
		err = proto.Unmarshal(message, pbBlock)
		if err != nil {
			return nil, fmt.Errorf("unable to read block proto: %s", err)
		}

		blk, err := NewBlockFromProto(pbBlock)
		if err != nil {
			return nil, err
		}

		return blk, nil
	}

	if err == io.EOF {
		return nil, err
	}

	// In all other cases, we are in an error path
	return nil, fmt.Errorf("failed reading next dbin message: %s", err)
}
