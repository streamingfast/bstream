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
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/dbin"
	pbblockmeta "github.com/streamingfast/pbgo/sf/blockmeta/v1"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type TestBlockIndexProvider struct {
	Blocks           []uint64
	LastIndexedBlock uint64
	ThrowError       error
}

func (t *TestBlockIndexProvider) BlocksInRange(lowBlockNum uint64, bundleSize uint64) (out []uint64, err error) {
	if t.ThrowError != nil {
		return nil, t.ThrowError
	}
	if lowBlockNum > t.LastIndexedBlock {
		return nil, fmt.Errorf("no indexed file here")
	}

	for _, blkNum := range t.Blocks {
		if blkNum >= lowBlockNum && blkNum < (lowBlockNum+bundleSize) {
			out = append(out, blkNum)
		}
	}

	return out, nil
}

func bRef(id string) BlockRef {
	return NewBlockRefFromID(id)
}

type TestSourceFactory struct {
	Created           chan *TestSource
	FromBlockNumFunc  func(uint64, Handler) Source
	FromCursorFunc    func(*Cursor, Handler) Source
	ThroughCursorFunc func(uint64, *Cursor, Handler) Source
	LowestBlkNum      uint64
}

func NewTestSourceFactory() *TestSourceFactory {
	return &TestSourceFactory{
		Created: make(chan *TestSource, 10),
	}
}

func (t *TestSourceFactory) LowestBlockNum() uint64 {
	return t.LowestBlkNum
}

func (t *TestSourceFactory) NewSource(h Handler) Source {
	src := NewTestSource(h)
	t.Created <- src
	return src
}

func (t *TestSourceFactory) NewSourceFromRef(ref BlockRef, h Handler) Source {
	src := NewTestSource(h)
	src.StartBlockID = ref.ID()
	t.Created <- src
	return src
}

func (t *TestSourceFactory) SourceFromBlockNum(blockNum uint64, h Handler) Source {
	if t.FromBlockNumFunc != nil {
		return t.FromBlockNumFunc(blockNum, h)
	}
	src := NewTestSource(h)
	src.StartBlockNum = blockNum
	t.Created <- src
	return src
}

func (t *TestSourceFactory) SourceFromCursor(cursor *Cursor, h Handler) Source {
	if t.FromCursorFunc != nil {
		return t.FromCursorFunc(cursor, h)
	}
	src := NewTestSource(h)
	src.Cursor = cursor
	t.Created <- src
	return src
}

func (t *TestSourceFactory) SourceThroughCursor(start uint64, cursor *Cursor, h Handler) Source {
	if t.ThroughCursorFunc != nil {
		return t.ThroughCursorFunc(start, cursor, h)
	}
	src := NewTestSource(h)
	src.StartBlockNum = start
	src.Cursor = cursor
	src.PassThroughCursor = true
	t.Created <- src
	return src
}

func NewTestSource(h Handler) *TestSource {
	return &TestSource{
		Shutter: shutter.New(),
		handler: h,
		running: make(chan interface{}),
		logger:  zlog,
	}
}

type TestSource struct {
	handler Handler
	logger  *zap.Logger
	*shutter.Shutter

	running           chan interface{}
	StartBlockID      string
	StartBlockNum     uint64
	Cursor            *Cursor
	PassThroughCursor bool
}

func (t *TestSource) SetLogger(logger *zap.Logger) {
	t.logger = logger
}

func (t *TestSource) Run() {
	close(t.running)
	<-t.Terminating()
}

func (t *TestSource) Push(b *Block, obj interface{}) error {
	err := t.handler.ProcessBlock(b, obj)
	if err != nil {
		t.Shutdown(err)
	}
	return err
}

var testBlockDateLayout = "2006-01-02T15:04:05.000"

func TestBlock(id, prev string) *Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev": %q}`, id, prev))
}

func TestBlockWithTimestamp(id, prev string, timestamp time.Time) *Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev":%q,"time":"%s"}`, id, prev, timestamp.Format(testBlockDateLayout)))
}

func TestBlockWithLIBNum(id, previousID string, newLIB uint64) *Block {
	return TestBlockFromJSON(TestJSONBlockWithLIBNum(id, previousID, newLIB))
}

func TestJSONBlockWithLIBNum(id, previousID string, newLIB uint64) string {
	return fmt.Sprintf(`{"id":%q,"prev":%q,"libnum":%d}`, id, previousID, newLIB)
}

type ParsableTestBlock struct {
	ID         string `json:"id,omitempty"`
	PreviousID string `json:"prev,omitempty"`
	Number     uint64 `json:"num,omitempty"`
	LIBNum     uint64 `json:"libnum,omitempty"`
	Timestamp  string `json:"time,omitempty"`
	Kind       int32  `json:"kind,omitempty"`
	Version    int32  `json:"version,omitempty"`
}

func TestBlockFromJSON(jsonContent string) *Block {

	obj := new(ParsableTestBlock)
	err := json.Unmarshal([]byte(jsonContent), obj)
	if err != nil {
		panic(fmt.Errorf("unable to read payload %q: %w", jsonContent, err))
	}

	blockTime := time.Time{}
	if obj.Timestamp != "" {
		t, err := time.Parse("2006-01-02T15:04:05.999", obj.Timestamp)
		if err != nil {
			panic(fmt.Errorf("unable to parse timestamp %q: %w", obj.Timestamp, err))
		}

		blockTime = t
	}

	number := obj.Number
	if number == 0 {
		number = blocknum(obj.ID)
	}
	GetBlockPayloadSetter = MemoryBlockPayloadSetter

	block := &Block{
		Id:         obj.ID,
		Number:     number,
		PreviousId: obj.PreviousID,
		Timestamp:  blockTime,
		LibNum:     obj.LIBNum,

		PayloadKind:    pbbstream.Protocol(obj.Kind),
		PayloadVersion: obj.Version,
	}
	block, err = GetBlockPayloadSetter(block, []byte(jsonContent))
	if err != nil {
		panic(err)
	}
	return block
}

// copies the eos behavior for simpler tests
func blocknum(blockID string) uint64 {
	b := blockID
	if len(blockID) < 8 { // shorter version, like 8a for 00000008a
		b = fmt.Sprintf("%09s", blockID)
	}
	bin, err := hex.DecodeString(b[:8])
	if err != nil {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(bin))
}

// Hopefully, this block kind value will never be used!
var TestProtocol = pbbstream.Protocol(0xEADBEEF)

var TestBlockReaderFactory = BlockReaderFactoryFunc(testBlockReaderFactory)

func testBlockReaderFactory(reader io.Reader) (BlockReader, error) {
	return &TestBlockReader{
		scanner: bufio.NewScanner(reader),
	}, nil
}

type TestBlockReader struct {
	scanner *bufio.Scanner
}

func (r *TestBlockReader) Read() (*Block, error) {
	success := r.scanner.Scan()
	if !success {
		err := r.scanner.Err()
		if err == nil {
			err = io.EOF
		}

		return nil, err
	}
	t := r.scanner.Text()
	return TestBlockFromJSON(t), nil
}

// Test Write simulate a blocker writer, you can use it in your test by
// assigning it in an init func like so:

type TestBlockWriterBin struct {
	DBinWriter *dbin.Writer
}

func (w *TestBlockWriterBin) Write(block *Block) error {
	pbBlock, err := block.ToProto()
	if err != nil {
		return err
	}

	bytes, err := proto.Marshal(pbBlock)
	if err != nil {
		return fmt.Errorf("unable to marshal proto block: %w", err)
	}

	return w.DBinWriter.WriteMessage(bytes)
}

type TestBlockReaderBin struct {
	DBinReader *dbin.Reader
}

func (l *TestBlockReaderBin) Read() (*Block, error) {
	message, err := l.DBinReader.ReadMessage()
	if len(message) > 0 {
		pbBlock := new(pbbstream.Block)
		err = proto.Unmarshal(message, pbBlock)
		if err != nil {
			return nil, fmt.Errorf("unable to read block proto: %w", err)
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

	return nil, fmt.Errorf("failed reading next dbin message: %w", err)
}

func TestIrrBlocksIdx(baseNum, bundleSize int, numToID map[int]string) (filename string, content []byte) {
	filename = fmt.Sprintf("%010d.%d.irr.idx", baseNum, bundleSize)

	var blockrefs []*pbblockmeta.BlockRef

	for i := baseNum; i < baseNum+bundleSize; i++ {
		if id, ok := numToID[i]; ok {
			blockrefs = append(blockrefs, &pbblockmeta.BlockRef{
				BlockNum: uint64(i),
				BlockID:  id,
			})
		}

	}

	var err error
	content, err = proto.Marshal(&pbblockmeta.BlockRefs{
		BlockRefs: blockrefs,
	})
	if err != nil {
		panic(err)
	}

	return
}
