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
	"encoding/json"
	"fmt"
	"io"
	"time"

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/dfuse-io/shutter"
	"github.com/eoscanada/eos-go"
)

func init() {
	AddBlockReaderFactory(TestProtocol, TestBlockReaderFactory)
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
	}
}

type TestSource struct {
	handler Handler
	*shutter.Shutter

	running       chan interface{}
	StartBlockID  string
	StartBlockNum uint64
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

// TestSubscriber instruments a Subscriber, implementing `Read()`
// and `Shutdown()`.
type TestSubscriber struct {
	*shutter.Shutter

	block         chan *Block
	err           chan error
	WeAreThereYet bool
}

///
/// Deprecated the rest down here?!
///

func NewTestSubscriber() *TestSubscriber {
	return &TestSubscriber{
		block:   make(chan *Block),
		err:     make(chan error),
		Shutter: shutter.New(),
	}
}

func (s *TestSubscriber) Read() (*Block, error) {
	zlog.Debug("mock subscriber asked to read()")
	select {
	case a := <-s.block:
		return a, nil
	case a := <-s.err:
		return nil, a
	}
}

func (s *TestSubscriber) Started() bool {
	return s.WeAreThereYet
}

func (s *TestSubscriber) GetBlockIDInBuffer(blockNum uint64) string {
	// FIXME stub
	return "fixme"
}
func (s *TestSubscriber) Start(channelSize int) {
	//FIXME STUB
}

func (s *TestSubscriber) StartAtBlockID(ID string) bool {
	return s.WeAreThereYet
}

func (s *TestSubscriber) WaitFor(ID string) <-chan interface{} {
	//FIXME stub
	return nil
}

func (s *TestSubscriber) PushBlock(blk *Block) {
	s.block <- blk
}

func (s *TestSubscriber) PushError(err error) {
	s.err <- err
}

// TestPipeline is an instrumented Pipeline object.
type TestPipeline struct {
	blk  chan *Block
	obj  chan interface{}
	errs chan error

	readTimeout time.Duration
}

func NewTestPipeline() *TestPipeline {
	return &TestPipeline{
		blk:         make(chan *Block, 100),
		obj:         make(chan interface{}, 100),
		errs:        make(chan error, 100),
		readTimeout: 100 * time.Millisecond,
	}
}

// ProcessBlock implements the `Pipeline` interface.
func (p *TestPipeline) ProcessBlock(blk *Block, obj interface{}) error {
	p.blk <- blk
	p.obj <- obj
	return <-p.errs
}

// Error consumes the next ProcessBlock and returns the provided
// error.
func (p *TestPipeline) Error(err error) (blk *Block, obj interface{}, readErr error) {
	select {
	case blk = <-p.blk:
	case <-time.After(p.readTimeout):
		return nil, nil, fmt.Errorf("TestPipeline read timed out")
	}
	obj = <-p.obj
	p.errs <- err
	return blk, obj, nil
}

// Next consumes the net block and provides a `nil` error.
func (p *TestPipeline) Next() (blk *Block, obj interface{}, err error) {
	select {
	case blk = <-p.blk:
	case <-time.After(p.readTimeout):
		return nil, nil, fmt.Errorf("TestPipeline read timed out")
	}
	obj = <-p.obj
	p.errs <- nil
	return blk, obj, nil
}

// TestPipelineMiddleware is a simplistic middleware with support only
// for ProcessBlock.  It does *not* handle FlushState (yet), nor
// PreprocessBlock.
type TestPipelineMiddleware struct {
	Pipeline
	afterProcessBlock TestAfterProcessBlockFunc
}

type TestAfterProcessBlockFunc func(blk *Block, obj interface{}, result error)

func NewTestPipelineMiddleware(child Pipeline, afterProcessBlock TestAfterProcessBlockFunc) *TestPipelineMiddleware {
	return &TestPipelineMiddleware{
		Pipeline:          child,
		afterProcessBlock: afterProcessBlock,
	}
}

func (p *TestPipelineMiddleware) ProcessBlock(blk *Block, obj interface{}) error {
	err := p.Pipeline.ProcessBlock(blk, obj)
	p.afterProcessBlock(blk, obj, err)
	return err
}

type TestPublisher struct {
	Blocks []*Block
}

func NewTestPublisher() *TestPublisher {
	return &TestPublisher{
		Blocks: []*Block{},
	}
}

func (p *TestPublisher) Publish(blk *Block) (relayed bool) {
	if blk != nil {
		p.Blocks = append(p.Blocks, blk)
		return true
	}

	return false
}

func (TestPublisher) Listen() error {
	return nil
}

var testBlockDateLayout = "2006-01-02T15:04:05.000"

func TestBlock(id, prev string) *Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev": %q}`, id, prev))
}

func TestBlockWithTimestamp(id, prev string, timestamp time.Time) *Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev":%q,"time":"%s"}`, id, prev, timestamp.Format(testBlockDateLayout)))
}

func TestBlockWithLIBNum(id, previousID string, newLIB uint64) *Block {
	return TestBlockFromJSON(fmt.Sprintf(`{"id":%q,"prev":%q,"libnum":%d}`, id, previousID, newLIB))
}

func TestBlockFromJSON(jsonContent string) *Block {

	type fields struct {
		ID         string `json:"id"`
		PreviousID string `json:"prev"`
		LIBNum     uint64 `json:"libnum"`
		Timestamp  string `json:"time"`
		Kind       int32  `json:"kind"`
		Version    int32  `json:"version"`
	}

	obj := new(fields)
	err := json.Unmarshal([]byte(jsonContent), obj)
	if err != nil {
		panic(fmt.Errorf("unable to read payload %q: %s", jsonContent, err))
	}

	blockTime := time.Time{}
	if obj.Timestamp != "" {
		t, err := time.Parse("2006-01-02T15:04:05.999", obj.Timestamp)
		if err != nil {
			panic(fmt.Errorf("unable to parse timestamp %q: %s", obj.Timestamp, err))
		}

		blockTime = t
	}

	return &Block{
		Id:         obj.ID,
		Number:     uint64(eos.BlockNum(obj.ID)),
		PreviousId: obj.PreviousID,
		Timestamp:  blockTime,
		LibNum:     obj.LIBNum,

		PayloadKind:    pbbstream.Protocol(obj.Kind),
		PayloadVersion: obj.Version,
		PayloadBuffer:  []byte(jsonContent),
	}
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
