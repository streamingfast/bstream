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

// The BlockWriterRegistry is required right now to support both old EOS
// format (JSON, accepted_block, text file, one per line) vs all the other upcoming
// ones that will start fresh with binary support for all file formats out of the box.
//
// When the EOS blocks and underlying data struvtures are converted to the new format,
// we will be able to remove the registry part and have a single writer implementation that
// is configured to write always in binary form through a `dbin` formatted file and a
// pre-configured block protocol.

type BlockWriter interface {
	Write(block *Block) error
}

type BlockWriterFactory interface {
	New(writer io.Writer) (BlockWriter, error)
}

type BlockWriterFactoryFunc func(writer io.Writer) (BlockWriter, error)

func (f BlockWriterFactoryFunc) New(writer io.Writer) (BlockWriter, error) {
	return f(writer)
}

var BlockWriterFactoryRegistry = map[pbbstream.Protocol]BlockWriterFactory{}

func AddBlockWriterFactory(protocol pbbstream.Protocol, factory BlockWriterFactory) {
	_, exists := BlockWriterFactoryRegistry[protocol]
	if exists {
		panic(fmt.Errorf("a block writer factory for protocol %s already exists, this is invalid", protocol))
	}

	BlockWriterFactoryRegistry[protocol] = factory
}

func MustGetBlockWriterFactory(protocol pbbstream.Protocol) BlockWriterFactory {
	factory := BlockWriterFactoryRegistry[protocol]
	if factory == nil {
		panic(fmt.Errorf("no block writer factory found for block protocol %s, factories are registered by importing specific protocol package, check that you imported such package in your app", protocol))
	}

	return factory
}
