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

package blockstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/streamingfast/bstream"
	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var exitError = errors.New("exit")

type mockBlocksClient struct {
	grpc.ClientStream
}

func (c *mockBlocksClient) Recv() (*pbbstream.Block, error) {
	return &pbbstream.Block{
		Number:    1,
		Id:        "00000001a",
		Timestamp: &timestamp.Timestamp{},
	}, nil
}

type mockBlockStreamClient struct {
}

func (c *mockBlockStreamClient) Blocks(ctx context.Context, in *pbbstream.BlockRequest, opts ...grpc.CallOption) (pbbstream.BlockStream_BlocksClient, error) {
	return &mockBlocksClient{}, nil
}

func testBlockStreamClient() pbbstream.BlockStreamClient {
	return &mockBlockStreamClient{}
}

func TestSourcePreprocessShutdown(t *testing.T) {

	var s *Source
	var resetableCounter int
	var dummyPreprocessor = bstream.PreprocessFunc(
		func(_ *pbbstream.Block) (interface{}, error) {
			return "", nil
		})

	tests := []struct {
		name                 string
		preprocFunc          bstream.PreprocessFunc
		preprocThreads       int
		handler              bstream.Handler
		expectedPreprocessed bool
		blocksToRead         int
		counterValidation    func(int)
		expectedError        error
	}{
		{
			name:           "not_stuck",
			preprocThreads: 3,
			preprocFunc:    dummyPreprocessor,
			handler: bstream.HandlerFunc(func(_ *pbbstream.Block, obj interface{}) error {
				require.NotNil(t, obj)
				resetableCounter++
				return nil
			}),
			counterValidation: func(counter int) {
				require.Greater(t, counter, 2)
			},
			expectedError: exitError,
		},
		{
			name:           "stuck_in_handler",
			preprocThreads: 3,
			preprocFunc:    dummyPreprocessor,
			handler: bstream.HandlerFunc(func(_ *pbbstream.Block, obj interface{}) error {
				require.NotNil(t, obj)
				time.Sleep(time.Millisecond * 10)
				resetableCounter++
				return nil
			}),
			counterValidation: func(counter int) {
				require.Equal(t, counter, 1)
			},
			expectedError: exitError,
		},
		{
			name:           "stuck_in_preprocessor",
			preprocThreads: 3,
			preprocFunc: bstream.PreprocessFunc(
				func(_ *pbbstream.Block) (interface{}, error) {
					time.Sleep(time.Second * 5)
					return "", nil
				}),
			handler: bstream.HandlerFunc(func(_ *pbbstream.Block, obj interface{}) error {
				require.NotNil(t, obj)
				resetableCounter++
				return nil
			}),
			counterValidation: func(counter int) {
				require.Equal(t, counter, 0)
			},
			expectedError: exitError,
		},
		{
			name:           "error_causes_no_more_processblock",
			preprocThreads: 3,
			preprocFunc:    dummyPreprocessor,
			handler: bstream.HandlerFunc(func(_ *pbbstream.Block, _ interface{}) error {
				resetableCounter++
				return errors.New("from_handler")
			}),
			counterValidation: func(counter int) {
				require.Equal(t, counter, 1)
			},
			expectedError: errors.New("from_handler"),
		},
		{
			name:           "processblocks_shutdown_causes_no_more_processblock",
			preprocThreads: 3,
			preprocFunc:    dummyPreprocessor,
			handler: bstream.HandlerFunc(func(_ *pbbstream.Block, _ interface{}) error {
				resetableCounter++
				if s.IsTerminating() {
					t.Error("should not come in here when shutdown has been called")
				}
				s.Shutdown(errors.New("from_handler"))
				return nil
			}),
			counterValidation: func(counter int) {
				require.Equal(t, counter, 1)
			},
			expectedError: errors.New("from_handler"),
		},
		{
			name:           "error_from_preprocessor",
			preprocThreads: 3,
			preprocFunc: bstream.PreprocessFunc(
				func(_ *pbbstream.Block) (interface{}, error) {
					return "", errors.New("from_preproc")
				}),
			handler: bstream.HandlerFunc(func(_ *pbbstream.Block, obj interface{}) error {
				require.NotNil(t, obj)
				resetableCounter++
				return nil
			}),
			counterValidation: func(counter int) {
				require.Equal(t, counter, 0)
			},
			expectedError: errors.New("from_preproc"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resetableCounter = 0

			s = &Source{
				Shutter:        shutter.New(),
				ctx:            context.Background(),
				handler:        test.handler,
				preprocFunc:    test.preprocFunc,
				preprocThreads: test.preprocThreads,
				logger:         zlog,
				requester:      "testRunClient",
			}

			time.AfterFunc(time.Millisecond*2, func() {
				s.Shutdown(exitError)
			})

			doneChan := make(chan interface{})
			go func() {
				err := s.run(testBlockStreamClient())
				require.Equal(t, test.expectedError, err)
				close(doneChan)
			}()

			select {
			case <-time.After(time.Second):
				t.Error("shutdown did not close s.run() correctly")
			case <-doneChan:
			}
			test.counterValidation(resetableCounter)
		})
	}
}

func TestSourceRunPreprocess(t *testing.T) {

	tests := []struct {
		name                 string
		preprocFunc          bstream.PreprocessFunc
		preprocThreads       int
		expectedPreprocessed bool
		blocksToRead         int
	}{
		{
			name:                 "with_preprocess",
			expectedPreprocessed: true,
			preprocThreads:       2,
			preprocFunc: bstream.PreprocessFunc(
				func(_ *pbbstream.Block) (interface{}, error) {
					return "", nil
				},
			),
			blocksToRead: 1,
		},
		{
			name:                 "without_preprocess",
			expectedPreprocessed: false,
			preprocThreads:       0,
			preprocFunc:          nil,
			blocksToRead:         1,
		},
		{
			name:                 "really_parallel",
			expectedPreprocessed: true,
			preprocThreads:       40,
			blocksToRead:         40,
			preprocFunc: bstream.PreprocessFunc(
				func(_ *pbbstream.Block) (interface{}, error) {
					time.Sleep(time.Millisecond * 2)
					return "", nil
				},
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ch := make(chan bool)
			var procFunc = func(_ *pbbstream.Block, obj interface{}) error {
				if obj == nil {
					ch <- false
				} else {
					ch <- true
				}
				return nil
			}

			s := &Source{
				Shutter:        shutter.New(),
				ctx:            context.Background(),
				handler:        bstream.HandlerFunc(procFunc),
				preprocFunc:    test.preprocFunc,
				preprocThreads: test.preprocThreads,
				logger:         zlog,
				requester:      "testRunClient",
			}

			go func() {
				err := s.run(testBlockStreamClient())
				require.NoError(t, err)
			}()

			timeoutChan := make(chan interface{})
			time.AfterFunc(15*time.Millisecond, func() {
				close(timeoutChan)
			})

			for i := 0; i < test.blocksToRead; i++ {
				select {
				case output := <-ch:
					require.Equal(t, output, test.expectedPreprocessed)
				case <-timeoutChan:
					t.Error("preprocessing or processing took too much time")
				}
			}
		})
	}

}
