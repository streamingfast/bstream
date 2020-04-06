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
	"bytes"
	"log"
	"math"
	"net/url"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/codecs/deos"
	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ship"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/gorilla/websocket"
)

func toDeos(from *ship.GetBlocksResultV0) *pbdeos.Block {
	traces := []*pbdeos.TransactionTrace{}
	for _, t := range from.Traces.AsTransactionTracesV0() {
		eosTrace := &eos.TransactionTrace{
			ID:        t.ID,
			BlockNum:  from.Head.BlockNum,
			BlockTime: from.Block.Timestamp,
			//			ProducerBlockID: , FIXME
			Receipt: &eos.TransactionReceiptHeader{
				Status:               t.Status,
				CPUUsageMicroSeconds: t.CPUUsageUS,
				NetUsageWords:        t.NetUsageWords,
			},
			Elapsed:   t.Elapsed,
			NetUsage:  eos.Uint64(t.NetUsage),
			Scheduled: t.Scheduled,
			//			AccountRamDelta: &struct {
			//				AccountName eos.AccountName `json:"account_name"`
			//				Delta       eos.Int64       `json:"delta"`
			//			}{
			//				AccountName: t.AccountDelta.Account,
			//				Delta:       t.AccountDelta.Delta,
			//			}, //FIXME getting nil pointer on t.AccountDelta
			//FailedDtrxTrace: t.FailedDtrxTrace, //FIXME variant...
		}
		errcode := eos.Uint64(t.ErrorCode)
		eosTrace.ErrorCode = &errcode

		actionTraces := []*eos.ActionTrace{}
		for _, atr := range t.ActionTraces {

			atrv0 := atr.Impl.(*ship.ActionTraceV0)

			var receipt *ship.ActionReceiptV0
			var actDigest string
			var globalSequence, receiveSequence, codeSequence, abiSequence eos.Uint64
			var receiver eos.AccountName
			if atrv0.Receipt != nil {
				receipt = atrv0.Receipt.Impl.(*ship.ActionReceiptV0)
				actDigest = receipt.ActDigest.String()
				globalSequence = eos.Uint64(receipt.GlobalSequence)
				receiveSequence = eos.Uint64(receipt.RecvSequence)
				codeSequence = eos.Uint64(receipt.CodeSequence)
				abiSequence = eos.Uint64(receipt.ABISequence)
				receiver = eos.AccountName(receipt.Receiver)
			}

			eosAct := &eos.Action{
				Account:       atrv0.Act.Account,
				Name:          atrv0.Act.Name,
				Authorization: atrv0.Act.Authorization,
				ActionData:    eos.NewActionDataFromHexData(atrv0.Act.Data),
			}
			errcode := eos.Uint64(atrv0.ErrorCode)
			eosActTrace := &eos.ActionTrace{
				Receipt: &eos.ActionTraceReceipt{
					Receiver:        receiver,
					ActionDigest:    actDigest,
					GlobalSequence:  globalSequence,
					ReceiveSequence: receiveSequence,
					CodeSequence:    codeSequence,
					ABISequence:     abiSequence,
				},
				Receiver:      eos.AccountName(atrv0.Receiver),
				Action:        eosAct,
				Elapsed:       eos.Int64(atrv0.Elapsed),
				Console:       atrv0.Console,
				TransactionID: t.ID,
				// InlineTraces:  // none of that on SHIP
				ContextFree: atrv0.ContextFree,
				BlockTime:   from.Block.Timestamp,
				BlockNum:    from.ThisBlock.BlockNum,
				// ProducerBlockID FIXME
				AccountRAMDeltas: atrv0.AccountRamDeltas,
				ErrorCode:        &errcode,
				//Except: atrv0.Except //FIXME
				ActionOrdinal:        uint32(atrv0.ActionOrdinal),
				CreatorActionOrdinal: uint32(atrv0.CreatorActionOrdinal),
				// ClosestUnnotifiedAncestorActionOrdinal: //FIXME
			}

			//AuthSequence: receipt.AuthSequence, FIXME
			actionTraces = append(actionTraces, eosActTrace)
		}

		//Except: eos.Except(t.Except), FIXME
		//ActionTraces: deos.ActionTraceToDEOS() FIXME
		traces = append(traces, deos.TransactionTraceToDEOS(eosTrace))
	}

	blk := from.Block.AsSignedBlock()
	trxs := []*pbdeos.TransactionReceipt{}
	for _, t := range blk.Transactions {
		preconvert := &eos.TransactionReceipt{
			TransactionReceiptHeader: t.TransactionReceiptHeader,
			Transaction:              eos.TransactionWithID{},
		}
		if t.Trx.TypeID == ship.TransactionIDType {
			if id, ok := t.Trx.Impl.(*eos.Checksum256); ok {
				preconvert.Transaction.ID = *id
			}
		} else if t.Trx.TypeID == ship.PackedTransactionType {
			if packed, ok := t.Trx.Impl.(*eos.PackedTransaction); ok {
				preconvert.Transaction.Packed = packed
				id, err := packed.ID()
				if err != nil {
					panic(err)
				}
				preconvert.Transaction.ID = id
			}
		}

		trxs = append(trxs, deos.TransactionReceiptToDEOS(preconvert))
	}
	ts, err := ptypes.TimestampProto(from.Block.Timestamp.Time)
	if err != nil {
		panic(err)
	}
	out := &pbdeos.Block{
		Id:     from.ThisBlock.BlockID.String(),
		Number: from.ThisBlock.BlockNum,
		Header: &pbdeos.BlockHeader{
			Previous:  from.PrevBlock.BlockID.String(),
			Timestamp: ts,
		},
		DposIrreversibleBlocknum: uint32(math.Min(float64(from.ThisBlock.BlockNum-1), float64(from.LastIrreversible.BlockNum))),
		ProducerSignature:        blk.ProducerSignature.String(),
		TransactionCount:         uint32(len(trxs)),
		Transactions:             trxs,
		TransactionTraceCount:    uint32(len(traces)),
		TransactionTraces:        traces,
	}

	return out
}

func DeosToBstreamBlock(in *pbdeos.Block) *bstream.Block {
	protoBlockData, err := proto.Marshal(in)
	if err != nil {
		panic("cannot marshal proto for block")
	}
	return &bstream.Block{
		Id:             in.Id,
		Number:         uint64(in.Number),
		PreviousId:     in.PreviousID(),
		Timestamp:      in.MustTime(),
		LibNum:         uint64(in.DposIrreversibleBlocknum),
		PayloadKind:    pbbstream.Protocol_EOS,
		PayloadVersion: 1,
		PayloadBuffer:  protoBlockData,
	}

}

func dial(addr string) (*websocket.Conn, *eos.ABI, error) {
	u, err := url.Parse(addr)
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, nil, err
	}

	var abi *eos.ABI
	abiBytes, err := readMessage(c)
	if err != nil {
		return nil, nil, err
	}
	abi, err = eos.NewABI(bytes.NewReader(abiBytes))
	return c, abi, err
}

func sendMessage(msg []byte, c *websocket.Conn) error {
	return c.WriteMessage(websocket.BinaryMessage, msg)
}

func readMessage(c *websocket.Conn) ([]byte, error) {
	_, message, err := c.ReadMessage()
	return message, err
}
