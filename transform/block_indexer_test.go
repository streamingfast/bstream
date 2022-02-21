package transform

import (
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestNewBlockIndexer(t *testing.T) {
	accountIndexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})

	indexer := NewBlockIndexer(accountIndexStore, 10, "")
	require.NotNil(t, indexer)
	require.IsType(t, BlockIndexer{}, *indexer)
	require.Equal(t, "default", indexer.IndexShortname)
}

func TestBlockIndexer_String(t *testing.T) {
	accountIndexStore := dstore.NewMockStore(func(base string, f io.Reader) error {
		return nil
	})

	indexer := NewBlockIndexer(accountIndexStore, 10, "")
	str := indexer.String()
	require.NotNil(t, str)
}

func TestBlockIndexer_WriteIndex(t *testing.T) {
	tests := []struct {
		name            string
		indexSize       uint64
		shouldWriteFile bool
		blocks          []*bstream.Block
	}{
		{
			name:            "sunny within bounds",
			indexSize:       10,
			shouldWriteFile: false,
			blocks:          []*bstream.Block{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

		})
	}
}

func TestBlockIndexer_ReadIndex(t *testing.T) {
	tests := []struct {
		name            string
		indexSize       uint64
		shouldWriteFile bool
		blocks          []*bstream.Block
	}{
		{
			name:            "sunny within bounds",
			indexSize:       10,
			shouldWriteFile: false,
			blocks:          []*bstream.Block{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

		})
	}
}
