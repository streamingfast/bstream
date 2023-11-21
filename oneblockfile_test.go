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

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOneBlockFile_MustNewOneBlockFile_Panics(t *testing.T) {
	name := "invalid-filename"
	defer func() { recover() }()
	_ = MustNewOneBlockFile(name)
}

func TestOneBlockFile_MustNewOneBlockFile(t *testing.T) {
	name := "0000000100-0000000000000100a-0000000000000099a-90-suffix"
	obf := MustNewOneBlockFile(name)
	require.IsType(t, OneBlockFile{}, *obf)
	require.Equal(t, obf.CanonicalName, strings.Split(name, "-suffix")[0])
}

func TestOneBlockFile_ParseFilename(t *testing.T) {
	tests := []struct {
		name                        string
		filename                    string
		expectBlockNum              uint64
		expectBlockIDSuffix         string
		expectPreviousBlockIDSuffix string
		expectCanonicalName         string
		expectError                 error
		expectLibNum                uint64
	}{
		{
			name:        "invalid",
			filename:    "invalid-filename",
			expectError: fmt.Errorf("wrong filename format: \"invalid-filename\""),
		},
		{
			name:        "without lib",
			filename:    "0000000100-aaaabbbb24a07267-ccccdddde5914b39",
			expectError: fmt.Errorf("wrong filename format: \"0000000100-aaaabbbb24a07267-ccccdddde5914b39\""),
		},
		{
			name:                        "with suffix",
			filename:                    "0000000100-aaaabbbb24a07267-ccccdddde5914b39-90-mind1",
			expectBlockNum:              100,
			expectLibNum:                90,
			expectBlockIDSuffix:         "aaaabbbb24a07267",
			expectPreviousBlockIDSuffix: "ccccdddde5914b39",
			expectCanonicalName:         "0000000100-aaaabbbb24a07267-ccccdddde5914b39-90",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blkNum, blkID, prevBlkID, libNum, name, err := ParseFilename(test.filename)
			if test.expectError != nil {
				require.Equal(t, err, test.expectError)
				return
			}
			require.Nil(t, err)
			assert.Equal(t, test.expectBlockNum, blkNum)
			assert.Equal(t, test.expectLibNum, libNum)
			assert.Equal(t, test.expectBlockIDSuffix, blkID)
			assert.Equal(t, test.expectPreviousBlockIDSuffix, prevBlkID)
			assert.Equal(t, test.expectCanonicalName, name)
		})
	}
}

func TestOneBlockFile_ParseFilename_InvalidBlockNum(t *testing.T) {
	name := "0000000FFF-24a07267aaaaeeee-e5914b39bbbbffff-suffix"
	_, _, _, _, _, err := ParseFilename(name)
	require.Error(t, err)
}

func TestOneBlockFile_ParseFilename_InvalidLibNum(t *testing.T) {
	name := "0000000100-24a07267aaaaeeee-e5914b39bbbb4444-FFFF-suffix"
	_, _, _, _, _, err := ParseFilename(name)
	require.Error(t, err)
}

func TestOneBlockFile_BlockFileName(t *testing.T) {
	block := &pbbstream.Block{
		Id:       "muchlongerthan16chars",
		Number:   uint64(0),
		ParentId: "muchlongerthan16charsalso",
		LibNum:   uint64(0),
	}
	bfn := BlockFileName(block)
	require.Equal(t, "0000000000-ongerthan16chars-rthan16charsalso-0-generated", bfn)
}
