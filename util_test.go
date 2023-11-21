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
	"errors"
	"testing"

	pbbstream "github.com/streamingfast/bstream/pb/sf/bstream/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDoForProtocol(t *testing.T) {
	kindString := ""
	assignEos := func() error { kindString = "EOS"; return nil }
	assignEth := func() error { return errors.New("failure") }

	require.NoError(t, DoForProtocol(pbbstream.Protocol_EOS, map[pbbstream.Protocol]func() error{
		pbbstream.Protocol_EOS: assignEos,
		pbbstream.Protocol_ETH: assignEth,
	}))

	assert.Equal(t, "EOS", kindString)

	require.Error(t, errors.New("failure"), DoForProtocol(pbbstream.Protocol_ETH, map[pbbstream.Protocol]func() error{
		pbbstream.Protocol_EOS: assignEos,
		pbbstream.Protocol_ETH: assignEth,
	}))

	// Should not have changed
	assert.Equal(t, "EOS", kindString)

	err := DoForProtocol(pbbstream.Protocol(10), map[pbbstream.Protocol]func() error{
		pbbstream.Protocol_EOS: assignEos,
		pbbstream.Protocol_ETH: assignEth,
	})
	require.EqualError(t, err, "don't know how to handle block kind 10")
}

func TestMustDoForProtocol(t *testing.T) {
	kindString := ""
	assignEos := func() { kindString = "EOS" }
	assignEth := func() { kindString = "ETH" }

	MustDoForProtocol(pbbstream.Protocol_EOS, map[pbbstream.Protocol]func(){
		pbbstream.Protocol_EOS: assignEos,
		pbbstream.Protocol_ETH: assignEth,
	})

	assert.Equal(t, "EOS", kindString)

	MustDoForProtocol(pbbstream.Protocol_ETH, map[pbbstream.Protocol]func(){
		pbbstream.Protocol_EOS: assignEos,
		pbbstream.Protocol_ETH: assignEth,
	})

	assert.Equal(t, "ETH", kindString)

	// I'm unable to make test check the actual error string value, something wrong with PanicsWithValue
	assert.Panics(t, func() {
		MustDoForProtocol(pbbstream.Protocol(10), map[pbbstream.Protocol]func(){
			pbbstream.Protocol_EOS: assignEos,
			pbbstream.Protocol_ETH: assignEth,
		})
	})
}
