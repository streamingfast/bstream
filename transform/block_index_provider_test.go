package transform

import (
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewBlockIndexProvider(t *testing.T) {
	indexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})
	filterFunc := func(index *BlockIndex) []uint64 {
		return nil
	}

	indexProvider := NewBlockIndexProvider(indexStore, filterFunc)
	require.NotNil(t, indexProvider)
	require.IsType(t, BlockIndexProvider{}, *indexProvider)
}
