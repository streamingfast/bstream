// Copyright 2021 dfuse Platform Inc.
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
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockWriterReader(t *testing.T) {
	// WRITE

	buffer := bytes.NewBuffer(nil)
	writer, err := NewDBinBlockWriter(buffer)
	require.NoError(t, err)

	block1Payload := []byte{0x0a, 0x0b, 0x0c}
	blk1 := &Block{
		Id:          "0a",
		Number:      1,
		PreviousId:  "0b",
		Timestamp:   time.Date(1970, time.December, 31, 19, 0, 0, 0, time.UTC),
		LibNum:      0,
		PayloadType: "test",
		GetPayload:  func() ([]byte, error) { return block1Payload, nil },
	}
	require.NoError(t, writer.Write(blk1))

	// READ

	reader, err := NewDBinBlockReader(bytes.NewReader(buffer.Bytes()), nil)
	require.NoError(t, err)

	readBlk1, err := reader.Read()
	require.Equal(t, blk1, readBlk1)
	require.NoError(t, err)

	readBlk2, err := reader.Read()
	assert.Nil(t, readBlk2)
	require.Equal(t, err, io.EOF)
}
