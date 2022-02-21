package transform

import (
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewBlockIndexer(t *testing.T) {
	accountIndexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})

	indexer := NewBlockIndexer(accountIndexStore, 10)
	require.NotNil(t, indexer)
	require.IsType(t, BlockIndexer{}, *indexer)

	str := indexer.String()
	require.NotNil(t, str)
}
