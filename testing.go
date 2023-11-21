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
	"testing"
	"time"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/dbin"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func (t *TestSource) Push(b *pbbstream.Block, obj interface{}) error {
	err := t.handler.ProcessBlock(b, obj)
	if err != nil {
		t.Shutdown(err)
	}
	return err
}

var testBlockDateLayout = "2006-01-02T15:04:05.000"

type ParsableTestBlock struct {
	ID        string `json:"id,omitempty"`
	ParentID  string `json:"prev,omitempty"`
	ParentNum uint64 `json:"prevnum,omitempty"`
	Number    uint64 `json:"num,omitempty"`
	LIBNum    uint64 `json:"libnum,omitempty"`
	Timestamp string `json:"time,omitempty"`
	Kind      int32  `json:"kind,omitempty"`
	Version   int32  `json:"version,omitempty"`
}

func TestBlock(id, prev string) *pbbstream.Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev": %q}`, id, prev))
}

func TestBlockWithNumbers(id, prev string, num, prevNum uint64) *pbbstream.Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev": %q,"prevnum": %d, "num": %d}`, id, prev, prevNum, num))
}

func TestBlockWithTimestamp(id, prev string, timestamp time.Time) *pbbstream.Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev":%q,"time":"%s"}`, id, prev, timestamp.Format(testBlockDateLayout)))
}

func TestBlockWithLIBNum(id, previousID string, newLIB uint64) *pbbstream.Block {
	return TestBlockFromJSON(TestJSONBlockWithLIBNum(id, previousID, newLIB))
}

func TestJSONBlockWithLIBNum(id, previousID string, newLIB uint64) string {
	return fmt.Sprintf(`{"id":%q,"prev":%q,"libnum":%d}`, id, previousID, newLIB)
}

func TestBlockFromJSON(jsonContent string) *pbbstream.Block {
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
	previousNum := obj.ParentNum
	if previousNum == 0 {
		previousNum = blocknum(obj.ParentID)
	}

	block := &pbbstream.Block{
		Id:        obj.ID,
		Number:    number,
		ParentId:  obj.ParentID,
		ParentNum: previousNum,
		Timestamp: timestamppb.New(blockTime),
		LibNum:    obj.LIBNum,
		Payload: &anypb.Any{
			TypeUrl: "type.googleapis.com/sf.bsream.type.v1.TestBlock",
			Value:   []byte(jsonContent),
		},
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

type TestBlockReader struct {
	scanner *bufio.Scanner
}

func (r *TestBlockReader) Read() (*pbbstream.Block, error) {
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

type TestBlockWriterBin struct {
	DBinWriter *dbin.Writer
}

func (w *TestBlockWriterBin) Write(blk *pbbstream.Block) error {
	bytes, err := proto.Marshal(blk)
	if err != nil {
		return fmt.Errorf("unable to marshal proto block: %w", err)
	}

	return w.DBinWriter.WriteMessage(bytes)
}

type TestBlockReaderBin struct {
	DBinReader *dbin.Reader
}

func (l *TestBlockReaderBin) Read() (*pbbstream.Block, error) {
	message, err := l.DBinReader.ReadMessage()
	if len(message) > 0 {
		blk := new(pbbstream.Block)
		err = proto.Unmarshal(message, blk)
		if err != nil {
			return nil, fmt.Errorf("unable to read block proto: %w", err)
		}

		return blk, nil
	}

	if err == io.EOF {
		return nil, err
	}

	return nil, fmt.Errorf("failed reading next dbin message: %w", err)
}

func AssertCursorEqual(t *testing.T, expected, actual *Cursor) {
	t.Helper()
	assert.Equal(t, expected.Step, actual.Step)
	AssertBlockRefEqual(t, expected.Block, actual.Block)
	AssertBlockRefEqual(t, expected.LIB, actual.LIB)
	AssertBlockRefEqual(t, expected.HeadBlock, actual.HeadBlock)
}

func AssertBlockRefEqual(t *testing.T, expected, actual BlockRef) {
	t.Helper()
	assert.Equal(t, expected.ID(), actual.ID())
	assert.Equal(t, expected.Num(), actual.Num())
}

func AssertProtoEqual(t *testing.T, expected, actual proto.Message) {
	t.Helper()

	// We use a custom comparison function and than rely on a standard `assert.Equal` so we get some
	// diffing information. Ideally, a better diff would be displayed, good enough for now.
	if !proto.Equal(expected, actual) {
		assert.Equal(t, expected, actual)
	}
}
