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
)

type BlockReader interface {
	Read() (*Block, error)
}

// DBinBlockReader reads pbbstream.Block from a stream source.
type DBinBlockReader struct {
	src            *dbin.Reader
	cacheBytesFunc CacheBytesFunc
}

func NewDBinBlockReader(reader io.Reader, cacher CacheBytesFunc) (out *DBinBlockReader, err error) {
	dbinReader := dbin.NewReader(reader)
	_, _, err = dbinReader.ReadHeader()
	if err != nil {
		return nil, fmt.Errorf("unable to read file header: %s", err)
	}
	return &DBinBlockReader{
		src: dbinReader,
	}, nil
}

// Read respects the `io.Reader` contract regarding `io.EOF`,
// i.e. it's possible to that both `block, io.EOF` be returned by a
// call to Read()
//
// You shall treat a non-nil block regardless of the `err` as if present,
// it's guaranteed it's valid. The subsequent call will still return `nil, io.EOF`.
// DBinBlockReader reads the dbin format where each element is assumed to be a `Block`.
func (l *DBinBlockReader) Read() (*Block, error) {
	message, err := l.src.ReadMessage()
	if len(message) > 0 {
		return NewBlockFromBytes(message, l.cacheBytesFunc)
	}
	if err == io.EOF {
		return nil, err
	}
	// In all other cases, we are in an error path
	return nil, fmt.Errorf("failed reading next dbin message: %w", err)
}
