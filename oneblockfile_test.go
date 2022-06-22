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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOneBlockFile_MustNewOneBlockFile_Panics(t *testing.T) {
	name := "invalid-filename"
	defer func() { recover() }()
	_ = MustNewOneBlockFile(name)
}

func TestOneBlockFile_MustNewOneBlockFile(t *testing.T) {
	name := "0000000100-20210728T105016.01-00000100a-00000099a-90-suffix"
	obf := MustNewOneBlockFile(name)
	require.IsType(t, OneBlockFile{}, *obf)
	require.Equal(t, obf.CanonicalName, strings.Split(name, "-suffix")[0])
}

func TestOneBlockFile_ParseFilename(t *testing.T) {
	lib := func(num uint64) *uint64 { lib := num; return &lib }
	tests := []struct {
		name                        string
		filename                    string
		expectBlockNum              uint64
		expectBlockTime             time.Time
		expectBlockIDSuffix         string
		expectPreviousBlockIDSuffix string
		expectCanonicalName         string
		expectError                 error
		expectLibNum                *uint64
	}{
		{
			name:        "invalid",
			filename:    "invalid-filename",
			expectError: fmt.Errorf("wrong filename format: \"invalid-filename\""),
		},
		{
			name:                        "without lib",
			filename:                    "0000000100-20170701T122141.0-24a07267-e5914b39",
			expectBlockNum:              100,
			expectLibNum:                nil,
			expectBlockTime:             mustParseTime("20170701T122141.0"),
			expectBlockIDSuffix:         "24a07267",
			expectPreviousBlockIDSuffix: "e5914b39",
			expectCanonicalName:         "0000000100-20170701T122141.0-24a07267-e5914b39",
		},
		{
			name:                        "with suffix",
			filename:                    "0000000100-20170701T122141.0-24a07267-e5914b39-90-mind1",
			expectBlockNum:              100,
			expectLibNum:                lib(90),
			expectBlockTime:             mustParseTime("20170701T122141.0"),
			expectBlockIDSuffix:         "24a07267",
			expectPreviousBlockIDSuffix: "e5914b39",
			expectCanonicalName:         "0000000100-20170701T122141.0-24a07267-e5914b39-90",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blkNum, blkTime, blkID, prevBlkID, libNum, name, err := ParseFilename(test.filename)
			if test.expectError != nil {
				require.Equal(t, err, test.expectError)
				return
			}
			require.Nil(t, err)
			assert.Equal(t, test.expectBlockNum, blkNum)
			assert.Equal(t, test.expectLibNum, libNum)
			assert.Equal(t, test.expectBlockTime, blkTime)
			assert.Equal(t, test.expectBlockIDSuffix, blkID)
			assert.Equal(t, test.expectPreviousBlockIDSuffix, prevBlkID)
			assert.Equal(t, test.expectCanonicalName, name)
		})
	}
}

func TestOneBlockFile_ParseFilename_InvalidBlockNum(t *testing.T) {
	name := "0000000FFF-20170701T122141.0-24a07267-e5914b39"
	_, _, _, _, _, _, err := ParseFilename(name)
	require.Error(t, err)
}

func TestOneBlockFile_ParseFilename_InvalidBlockTime(t *testing.T) {
	name := "0000000100-20ABCDE1T122141.0-24a07267-e5914b39"
	_, _, _, _, _, _, err := ParseFilename(name)
	require.Error(t, err)
}

func TestOneBlockFile_ParseFilename_InvalidLibNum(t *testing.T) {
	name := "0000000100-20170701T122141.0-24a07267-e5914b39-FFFF-suffix"
	_, _, _, _, _, _, err := ParseFilename(name)
	require.Error(t, err)
}

func mustParseTime(in string) time.Time {
	t, err := time.Parse("20060102T150405.999999", in)
	if err != nil {
		panic("invalid parsetime")
	}
	return t
}

func TestOneBlockFile_InnerLibNumPanic(t *testing.T) {
	name := "0000000100-20210728T105016.01-00000100a-00000099a-90-suffix"
	obf := MustNewOneBlockFile(name)

	defer func() { recover() }()
	obf.InnerLibNum = nil
	_ = obf.LibNum()
}

func newBstreamBlock() *Block {
	block := Block{
		Id:         "longerthan8",
		Number:     uint64(0),
		PreviousId: "longerthan8too",
		Timestamp:  mustParseTime("20170701T122141.0"),
		LibNum:     uint64(0),
	}

	return &block
}

func TestOneBlockFile_BlockFileName(t *testing.T) {
	block := newBstreamBlock()
	bfn := BlockFileName(block)
	require.Equal(t, bfn, "0000000000-20170701T122141.0-gerthan8-than8too-0-generated")
}
