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
	"context"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/streamingfast/dstore"
)

type OneBlockDownloaderFunc = func(ctx context.Context, oneBlockFile *OneBlockFile) (data []byte, err error)

// OneBlockFile is the representation of a single block inside one or more duplicate files, before they are merged
// It has a truncated ID
type OneBlockFile struct {
	CanonicalName string
	Filenames     map[string]bool
	BlockTime     time.Time
	ID            string
	Num           uint64
	InnerLibNum   *uint64 //never use this field directly
	PreviousID    string
	MemoizeData   []byte
	Deleted       bool
}

// ToBstreamBlock is used to create a dummy "empty" block to use in a ForkableHandler
func (f *OneBlockFile) ToBstreamBlock() *Block {
	blk := &Block{
		Id:         f.ID,
		Number:     f.Num,
		PreviousId: f.PreviousID,
		Timestamp:  f.BlockTime,
	}

	if f.InnerLibNum != nil {
		blk.LibNum = *f.InnerLibNum
	}
	return blk
}

func (f *OneBlockFile) String() string {
	return fmt.Sprintf("#%d (ID %s, ParentID %s)", f.Num, f.ID, f.PreviousID)
}

func NewOneBlockFile(fileName string) (*OneBlockFile, error) {
	_ = &Block{}
	blockNum, blockTime, blockID, previousBlockID, libNum, canonicalName, err := ParseFilename(fileName)
	if err != nil {
		return nil, err
	}
	return &OneBlockFile{
		CanonicalName: canonicalName,
		Filenames: map[string]bool{
			fileName: true,
		},
		BlockTime:   blockTime,
		ID:          blockID,
		Num:         blockNum,
		PreviousID:  previousBlockID,
		InnerLibNum: libNum,
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
	if len(f.MemoizeData) == 0 {
		data, err := oneBlockDownloader(ctx, f)
		if err != nil {
			return nil, err
		}
		f.MemoizeData = data
	}
	return f.MemoizeData, nil
}

func (f *OneBlockFile) LibNum() uint64 {
	if f.InnerLibNum == nil {
		panic("one block file lib num not set")
	}
	return *f.InnerLibNum
}

// ParseFilename parses file names formatted like:
// * 0000000101-20170701T122141.5-dbda3f44-24a07267-100-mindread1
// * 0000000101-20170701T122141.5-dbda3f44-24a07267-100-mindread2

func ParseFilename(filename string) (blockNum uint64, blockTime time.Time, blockIDSuffix string, previousBlockIDSuffix string, libNum *uint64, canonicalName string, err error) {
	parts := strings.Split(filename, "-")
	if len(parts) < 4 || len(parts) > 6 {
		err = fmt.Errorf("wrong filename format: %q", filename)
		return
	}

	blockNumVal, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		err = fmt.Errorf("failed parsing %q: %s", parts[0], err)
		return
	}
	blockNum = blockNumVal

	blockTime, err = time.Parse("20060102T150405.999999", parts[1])
	if err != nil {
		err = fmt.Errorf("failed parsing %q: %s", parts[1], err)
		return
	}

	blockIDSuffix = parts[2]
	previousBlockIDSuffix = parts[3]
	canonicalName = filename
	if len(parts) == 6 {
		libNumVal, parseErr := strconv.ParseUint(parts[4], 10, 32)
		if parseErr != nil {
			err = fmt.Errorf("failed parsing lib num %q: %s", parts[4], parseErr)
			return
		}
		libNum = &libNumVal
		canonicalName = strings.Join(parts[0:5], "-")
	}

	return
}

func BlockFileName(block *Block) string {
	return BlockFileNameWithSuffix(block, "generated")
}

func TruncateBlockID(in string) string {
	if len(in) <= 8 {
		return in
	}
	return in[len(in)-8:]
}

func BlockFileNameWithSuffix(block *Block, suffix string) string {
	blockTime := block.Time()
	blockTimeString := fmt.Sprintf("%s.%01d", blockTime.Format("20060102T150405"), blockTime.Nanosecond()/100000000)

	blockID := TruncateBlockID(block.ID())
	previousID := TruncateBlockID(block.PreviousID())

	return fmt.Sprintf("%010d-%s-%s-%s-%d-%s", block.Num(), blockTimeString, blockID, previousID, block.LibNum, suffix)
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
