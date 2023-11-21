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
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBlockWriter(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{})
	blockWriter, err := NewDBinBlockWriter(buffer)
	require.NoError(t, err)

	block1Payload := []byte{0x0a, 0x0b, 0x0c}

	blk1 := &pbbstream.Block{
		Id:        "0a",
		Number:    1,
		ParentId:  "0b",
		Timestamp: timestamppb.New(time.Date(1970, time.December, 31, 19, 0, 0, 0, time.UTC)),
		LibNum:    0,
		Payload: &anypb.Any{
			TypeUrl: "type.googleapis.com/sf.bstream.type.v1.TestBlock",
			Value:   block1Payload,
		},
	}

	err = blockWriter.Write(blk1)
	require.NoError(t, err)

	// Reader part (to validate the data)
	blockReader, err := NewDBinBlockReader(buffer)
	require.NoError(t, err)

	readBlk1, err := blockReader.Read()
	require.NoError(t, err)
	AssertProtoEqual(t, blk1, readBlk1)

}
