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

package shipreader

import (
	"github.com/dfuse-io/shutter"
	"github.com/dfuse-io/bstream"
	"github.com/eoscanada/eos-go/ship"
)

type ShipReader struct {
	*shutter.Shutter
	addr          string
	startBlockNum uint32
	handler       bstream.Handler
}

func (s *ShipReader) Run() {
	s.Shutdown(s.run())
}

func (s *ShipReader) run() error {

	c, _, err := dial(s.addr)
	if err != nil {
		return err
	}
	req := ship.NewRequest(&ship.GetBlocksRequestV0{
		StartBlockNum:       s.startBlockNum,
		EndBlockNum:         0xffffffff,
		MaxMessagesInFlight: 5,
		FetchBlock:          true,
		FetchTraces:         true,
		FetchDeltas:         false,
	})
	if err := sendMessage(req, c); err != nil {
		return err
	}

	for {
		if s.IsTerminating() {
			return s.Err()
		}
		msg, err := readMessage(c)
		if err != nil {
			return err
		}

		result, err := ship.ParseGetBlockResultV0(msg)
		if err != nil {
			return err
		}

		ack := ship.NewGetBlocksAck(1)
		err = sendMessage(ack, c)
		if err != nil {
			return err
		}

		deosBlock := toDeos(result)
		blk := DeosToBstreamBlock(deosBlock)
		if err := s.handler.ProcessBlock(blk, nil); err != nil {
			return err
		}
	}

}

func New(addr string, startBlockNum uint32, h bstream.Handler) *ShipReader {
	s := &ShipReader{
		Shutter:       shutter.New(),
		startBlockNum: startBlockNum,
		handler:       h,
		addr:          addr,
	}
	return s
}
