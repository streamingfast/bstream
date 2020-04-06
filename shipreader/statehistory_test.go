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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dfuse-io/logging"

	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	"github.com/eoscanada/eos-go/ship"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func init() {
	if os.Getenv("DEBUG") != "" {
		logger := logging.MustCreateLoggerWithLevel("test", zap.NewAtomicLevelAt(zap.DebugLevel))
		logging.Override(logger)
	}
}

func Test_sendMessage(t *testing.T) {
	if os.Getenv("TEST_SHIP") == "" {
		t.Skip()
	}

	c, _, err := dial("ws://localhost:8080")
	require.NoError(t, err)

	req := ship.NewRequest(&ship.GetBlocksRequestV0{
		StartBlockNum:       10,
		EndBlockNum:         0xffffffff,
		MaxMessagesInFlight: 10,
		FetchBlock:          true,
		FetchTraces:         true,
		FetchDeltas:         true,
	})

	err = sendMessage(req, c)
	require.NoError(t, err)

	for {
		msg, err := readMessage(c)
		require.NoError(t, err)

		result, err := ship.ParseGetBlockResultV0(msg)
		require.NoError(t, err)

		ack := ship.NewGetBlocksAck(1)
		err = sendMessage(ack, c)
		require.NoError(t, err)

		//fmt.Println(result)
		//fmt.Printf("blk: %+v\n", result.Block.AsSignedBlock())
		//for _, tt := range result.Traces.AsTransactionTracesV0() {
		//	fmt.Printf("tr: %+v\n", tt)
		//}

		deosBlock := toDeos(result)
		blk := DeosToBstreamBlock(deosBlock)
		native := blk.ToNative().(*pbdeos.Block)
		require.NoError(t, err)
		//	fmt.Printf("block: %+v\n", native)
		for _, t := range native.Transactions {
			fmt.Printf("id is: %s\n", t.Id)
		}
		for _, t := range result.Deltas.Elem {
			fmt.Println("printing")
			fmt.Println(t.Impl)
		}

		time.Sleep(time.Second)
	}

	//	res := &Result{}
	//	err = eos.UnmarshalBinary(msg, &res)
	//	require.NoError(t, err)
	//
	//	typedRes := res.Impl.(*GetBlocksResultV0)
	//	fmt.Printf("%+v\n", typedRes)
	//	fmt.Printf("%+v\n", typedRes.LastIrreversible)
	//	//typedRes.Traces
	//	decodedTraces := []*eos.TransactionTrace{}
	//	fmt.Println("hexencoded")
	//	fmt.Println(hex.EncodeToString(typedRes.Traces))
	//	fmt.Println("end")
	//
	//	eos.UnmarshalBinary(typedRes.Traces, decodedTraces)
	//	// FIXME this will be an array...
	//	decoded, err := abi.Decode(eos.NewDecoder(typedRes.Traces[2:]), "transaction_trace_v0")
	//	require.NoError(t, err)
	//	fmt.Println("jsondecoded", string(decoded))
	//
	//	decodedBlock, err := abi.Decode(eos.NewDecoder(typedRes.Block), "signed_block")
	//	require.NoError(t, err)
	//	fmt.Println("jsondecodedblock", string(decodedBlock))
	//
	//
	//
	//	spew.Dump(typedRes.Traces)
	//
	//	fmt.Println(typedRes.ThisBlock.BlockNum, typedRes.ThisBlock.BlockID)
	//	spew.Dump(decodedTraces)

	//fmt.Println("len act traces:", len(decodedTraces[0].ActionTraces))

	//fmt.Println(decodedTraces[0].ID)

	// here: look at first byte, then decode...

	//decoded, err := abi.Decode(eos.NewDecoder(msg[1:]), "get_blocks_result_v0")
	//require.NoError(t, err)
	//fmt.Println("decoded: ", string(decoded))

}

//[ 'get_blocks_ack_request_v0',
//  { start_block_num: 99999,
//    end_block_num: 4294967295,
//    max_messages_in_flight: 5,
//    have_positions: [],
//    irreversible_only: false,
//    fetch_block: true,
//    fetch_traces: true,
//    fetch_deltas: true } ]
