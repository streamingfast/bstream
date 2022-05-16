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
	"google.golang.org/protobuf/proto"
)

// The BlockWriterRegistry is required right now to support both old EOS
// format (JSON, accepted_block, text file, one per line) vs all the other upcoming
// ones that will start fresh with binary support for all file formats out of the box.
//
// When the EOS blocks and underlying data struvtures are converted to the new format,
// we will be able to remove the registry part and have a single writer implementation that
// is configured to write always in binary form through a `dbin` formatted file and a
// pre-configured block protocol.

type BlockWriter interface {
	Write(block *Block) error
}

type BlockWriterFactory func(writer io.Writer) (BlockWriter, error)

var _ BlockWriter = (*DBinBlockWriter)(nil)

// DBinBlockWriter reads the dbin format where each element is assumed to be a `Block`.
type DBinBlockWriter struct {
	src *dbin.Writer
}

// NewDBinBlockWriter creates a new DBinBlockWriter that writes to 'dbin' format, the 'contentType'
// must be 3 characters long perfectly, version should represents a version of the content.
func NewDBinBlockWriter(writer io.Writer) (*DBinBlockWriter, error) {
	dbinWriter := dbin.NewWriter(writer)
	err := dbinWriter.WriteHeader("BST", 1)
	if err != nil {
		return nil, fmt.Errorf("unable to write file header: %w", err)
	}

	return &DBinBlockWriter{
		src: dbinWriter,
	}, nil
}

func (w *DBinBlockWriter) Write(block *Block) error {
	pbBlock, err := block.ToProto()
	if err != nil {
		return err
	}

	bytes, err := proto.Marshal(pbBlock)
	if err != nil {
		return fmt.Errorf("unable to marshal proto block: %w", err)
	}

	return w.src.WriteMessage(bytes)
}
