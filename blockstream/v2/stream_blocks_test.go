package blockstream

import (
	"context"
	"strings"
	"testing"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/test-go/testify/assert"
	"github.com/test-go/testify/require"
	"go.uber.org/zap"
)

func TestLocalBlocks(t *testing.T) {

	store := dstore.NewMockStore(nil)
	blocksStores := []dstore.Store{store}
	logger := zap.NewNop()

	s := NewServer(
		logger,
		blocksStores,
		nil,
		nil,
		nil,
		nil,
	)

	// fake block decoder func to return bstream.Block
	bstream.GetBlockDecoder = bstream.BlockDecoderFunc(func(blk *bstream.Block) (interface{}, error) {
		block := new(pbbstream.Block)
		block.Number = blk.Number
		block.Id = blk.Id
		block.PreviousId = blk.PreviousId
		return block, nil
	})

	blocks := strings.Join([]string{
		bstream.TestJSONBlockWithLIBNum("00000002a", "00000001a", 1),
		bstream.TestJSONBlockWithLIBNum("00000003a", "00000002a", 1),
		bstream.TestJSONBlockWithLIBNum("00000004a", "00000003a", 1),
		bstream.TestJSONBlockWithLIBNum("00000005a", "00000004a", 1), // last irreversible closes on endblock
	}, "\n")

	store.SetFile("0000000000", []byte(blocks))

	localClient := s.BlocksFromLocal(context.Background(), &pbbstream.BlocksRequestV2{
		StartBlockNum: 2,
		StopBlockNum:  3,
		Confirmations: 1,
	})

	blk, err := localClient.Recv()
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_NEW)
	require.NoError(t, err)

	blk, err = localClient.Recv()
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_IRREVERSIBLE)
	require.NoError(t, err)

	blk, err = localClient.Recv()
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_NEW)
	require.NoError(t, err)

	blk, err = localClient.Recv()
	assert.Equal(t, blk.Step, pbbstream.ForkStep_STEP_IRREVERSIBLE)
	require.NoError(t, err)

	blk, err = localClient.Recv()
	assert.Nil(t, blk)
	require.NoError(t, err)

}
