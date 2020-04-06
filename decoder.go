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

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
)

type BlockDecoder interface {
	Decode(blk *Block) (interface{}, error)
}

type BlockDecoderFunc func(blk *Block) (interface{}, error)

func (f BlockDecoderFunc) Decode(blk *Block) (interface{}, error) {
	return f(blk)
}

var BlockDecoderRegistry = map[pbbstream.Protocol]BlockDecoder{}

func AddBlockDecoder(protocol pbbstream.Protocol, decoder BlockDecoder) {
	_, exists := BlockDecoderRegistry[protocol]
	if exists {
		panic(fmt.Errorf("a block decoder for protocol %s already exists, this is invalid", protocol))
	}

	BlockDecoderRegistry[protocol] = decoder
}

func MustGetBlockDecoder(protocol pbbstream.Protocol) BlockDecoder {
	decoder := BlockDecoderRegistry[protocol]
	if decoder == nil {
		panic(fmt.Errorf("no block decoder found for block protocol %s, check that you underscore-import the right package (bstream/codecs/deos, bstream/codecs/deth, etc.)", protocol))
	}

	return decoder
}
