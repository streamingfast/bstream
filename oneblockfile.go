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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"

	"github.com/streamingfast/dstore"
)

type OneBlockDownloaderFunc = func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error)

func decodeOneblockfileData(data []byte) (*Block, error) {
	reader := bytes.NewReader(data)
	blockReader, err := getBlockReaderFactory().New(reader)
	if err != nil {
		return nil, fmt.Errorf("unable to create block reader: %w", err)
	}
	blk, err := blockReader.Read()
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("block reader failed: %w", err)
	}
	return blk, nil
}

// OneBlockFile is the representation of a single block inside one or more duplicate files, before they are merged
// It has a truncated ID
type OneBlockFile struct {
	sync.Mutex
	CanonicalName string
	Filenames     map[string]bool
	ID            string
	Num           uint64
	LibNum        uint64
	PreviousID    string
	MemoizeData   []byte
	Deleted       bool
}

func (f *OneBlockFile) ToBstreamBlock() *Block {
	return &Block{
		Id:         f.ID,
		Number:     f.Num,
		PreviousId: f.PreviousID,
		LibNum:     f.LibNum,
	}
}

func (f *OneBlockFile) String() string {
	return fmt.Sprintf("#%d (ID %s, ParentID %s)", f.Num, f.ID, f.PreviousID)
}

func NewOneBlockFile(fileName string) (*OneBlockFile, error) {
	_ = &Block{}
	blockNum, blockID, previousBlockID, libNum, canonicalName, err := ParseFilename(fileName)
	if err != nil {
		return nil, err
	}
	return &OneBlockFile{
		CanonicalName: canonicalName,
		Filenames: map[string]bool{
			fileName: true,
		},
		ID:         blockID,
		Num:        blockNum,
		PreviousID: previousBlockID,
		LibNum:     libNum,
	}, nil
}

func MustNewOneBlockFile(fileName string) *OneBlockFile {
	out, err := NewOneBlockFile(fileName)
	if err != nil {
		panic(err)
	}
	return out
}

func (f *OneBlockFile) Data(ctx context.Context, oneBlockDownloader OneBlockDownloaderFunc) ([]byte, error) {
	f.Lock()
	defer f.Unlock()
	if len(f.MemoizeData) == 0 {
		data, err := oneBlockDownloader(ctx, f)
		if err != nil {
			return nil, err
		}
		f.MemoizeData = data
	}
	return f.MemoizeData, nil
}

// ParseFilename parses file names formatted like:
// * 0000000101-dbda3f44afee24dd-24a072678473e4ad-100-mindread1
// * 0000000101-dbda3f44afee24dd-24a072678473e4ad-100-mindread2

func ParseFilename(filename string) (blockNum uint64, blockIDSuffix string, previousBlockIDSuffix string, libNum uint64, canonicalName string, err error) {
	parts := strings.Split(filename, "-")
	if len(parts) != 5 {
		err = fmt.Errorf("wrong filename format: %q", filename)
		return
	}

	blockNumVal, parseErr := strconv.ParseUint(parts[0], 10, 32)
	if parseErr != nil {
		err = fmt.Errorf("failed parsing %q: %s", parts[0], parseErr)
		return
	}
	blockNum = blockNumVal

	blockIDSuffix = parts[1]
	previousBlockIDSuffix = parts[2]
	canonicalName = filename
	libNum, parseErr = strconv.ParseUint(parts[3], 10, 32)
	if parseErr != nil {
		err = fmt.Errorf("failed parsing lib num %q: %s", parts[4], parseErr)
		return
	}
	canonicalName = strings.Join(parts[0:4], "-")

	return
}

func BlockFileName(block *Block) string {
	return BlockFileNameWithSuffix(block, "generated")
}

func TruncateBlockID(in string) string {
	if len(in) <= 16 {
		return in
	}
	return in[len(in)-16:]
}

func BlockFileNameWithSuffix(block *Block, suffix string) string {
	blockID := TruncateBlockID(block.ID())
	previousID := TruncateBlockID(block.PreviousID())

	return fmt.Sprintf("%010d-%s-%s-%d-%s", block.Num(), blockID, previousID, block.LibNum, suffix)
}

func OneBlockDownloaderFromStore(blocksStore dstore.Store) OneBlockDownloaderFunc {
	return func(ctx context.Context, obf *OneBlockFile) ([]byte, error) {
		for filename := range obf.Filenames {
			reader, err := blocksStore.OpenObject(ctx, filename)
			if err != nil {
				return nil, fmt.Errorf("fetching %s from block store: %w", filename, err)
			}
			defer reader.Close()

			return ioutil.ReadAll(reader)
		}
		return nil, fmt.Errorf("no filename for this oneBlockFile")
	}
}
