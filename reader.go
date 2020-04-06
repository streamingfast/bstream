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
	"io"

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
)

// BlockReader is a reader protocol reading out bstream `Block` from a
// stream source. The reader respects the `io.Reader` contract in
// respect to `io.EOF`, i.e. it's possible to that both `block, io.EOF`
// be returned by the reader.
//
// You shall treat a non-nil block regardless of the `err` as if present,
// it's guaranteed it's valid. The subsequent call will still return `nil, io.EOF`.
type BlockReader interface {
	Read() (*Block, error)
}

type BlockReaderFactory interface {
	New(reader io.Reader) (BlockReader, error)
}

type BlockReaderFactoryFunc func(reader io.Reader) (BlockReader, error)

func (f BlockReaderFactoryFunc) New(reader io.Reader) (BlockReader, error) {
	return f(reader)
}

// The BlockReaderRegistry is required right now to support both old EOS
// format (JSON, accepted_block, text file, one per line) vs all the other upcoming
// ones that will start fresh with binary support for all file formats out of the box.
//
// When the EOS blocks and underlying data struvtures are converted to the new format,
// we will be able to remove the registry part and have a single reader implementation that
// is configured to read always in binary form through a `dbin` formatted file and a
// pre-configured block protocol.

var BlockReaderFactoryRegistry = map[pbbstream.Protocol]BlockReaderFactory{}

func AddBlockReaderFactory(protocol pbbstream.Protocol, factory BlockReaderFactory) {
	_, exists := BlockReaderFactoryRegistry[protocol]
	if exists {
		panic(fmt.Errorf("a block reader factory for protocol %s already exists, this is invalid", protocol))
	}

	BlockReaderFactoryRegistry[protocol] = factory
}

func MustGetBlockReaderFactory(protocol pbbstream.Protocol) BlockReaderFactory {
	factory := BlockReaderFactoryRegistry[protocol]
	if factory == nil {
		panic(fmt.Errorf("no block reader factory found for block protocol %s, check that you underscore-import the right package (bstream/codecs/deos, bstream/codecs/deth, etc.)", protocol))
	}

	return factory
}
