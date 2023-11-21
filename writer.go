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
	"google.golang.org/protobuf/proto"

	"github.com/streamingfast/dbin"
)

// DBinBlockWriter reads the dbin format where each element is assumed to be a `Block`.
type DBinBlockWriter struct {
	src              *dbin.Writer
	hasWrittenHeader bool
}

// NewDBinBlockWriter creates a new DBinBlockWriter that writes to 'dbin' format, the 'contentType'
// must be 3 characters long perfectly, version should represent a version of the content.
func NewDBinBlockWriter(writer io.Writer) (*DBinBlockWriter, error) {
	dbinWriter := dbin.NewWriter(writer)

	return &DBinBlockWriter{
		src: dbinWriter,
	}, nil
}

func (w *DBinBlockWriter) Write(block *pbbstream.Block) error {
	if !w.hasWrittenHeader {
		err := w.src.WriteHeader(block.Payload.TypeUrl)
		if err != nil {
			return fmt.Errorf("unable to write file header: %s", err)
		}
		w.hasWrittenHeader = true
	}

	bytes, err := proto.Marshal(block)
	if err != nil {
		return fmt.Errorf("unable to marshal proto block: %s", err)
	}

	return w.src.WriteMessage(bytes)
}
