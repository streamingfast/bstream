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

import pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"

type BlockDecoder interface {
	Decode(blk *pbbstream.Block) (interface{}, error)
}

type BlockDecoderFunc func(blk *pbbstream.Block) (interface{}, error)

func (f BlockDecoderFunc) Decode(blk *pbbstream.Block) (interface{}, error) {
	return f(blk)
}
