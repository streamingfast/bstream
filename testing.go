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

	"github.com/streamingfast/dbin"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/streamingfast/shutter"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func init() {
	GetBlockReaderFactory = TestBlockReaderFactory
}

type TestSourceFactory struct {
	Created chan *TestSource
}

func NewTestSourceFactory() *TestSourceFactory {
	return &TestSourceFactory{
		Created: make(chan *TestSource, 10),
	}
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

func (t *TestSourceFactory) NewSourceFromNum(blockNum uint64, h Handler) Source {
	src := NewTestSource(h)
	src.StartBlockNum = blockNum
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

	running       chan interface{}
	StartBlockID  string
	StartBlockNum uint64
}

func (t *TestSource) SetLogger(logger *zap.Logger) {
	t.logger = logger
}

func (t *TestSource) Run() {
	close(t.running)
	<-t.Terminating()
}

func (t *TestSource) Push(b *Block, obj interface{}) error {
	// FIXME: should we handle the error here? and fail the TestSource
	// if the downstream handler fails?
	return t.handler.ProcessBlock(b, obj)
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

func TestBlockFromJSON(jsonContent string) *Block {

	type fields struct {
		ID         string `json:"id"`
		PreviousID string `json:"prev"`
		Number     uint64 `json:"num"`
		LIBNum     uint64 `json:"libnum"`
		Timestamp  string `json:"time"`
		Kind       int32  `json:"kind"`
		Version    int32  `json:"version"`
	}

	obj := new(fields)
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
	return &Block{
		Id:         obj.ID,
		Number:     number,
		PreviousId: obj.PreviousID,
		Timestamp:  blockTime,
		LibNum:     obj.LIBNum,

		PayloadKind:    pbbstream.Protocol(obj.Kind),
		PayloadVersion: obj.Version,
		PayloadBuffer:  []byte(jsonContent),
	}
}

// copies the eos behavior for simpler tests
func blocknum(blockID string) uint64 {
	if len(blockID) < 8 {
		return 0
	}
	bin, err := hex.DecodeString(blockID[:8])
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

		blk, err := BlockFromProto(pbBlock)
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
