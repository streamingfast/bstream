package bstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type oneBlocksSource struct {
	*shutter.Shutter
	oneBlockFiles []*OneBlockFile
	downloader    OneBlockDownloaderFunc
	handler       Handler
	ctx           context.Context
}

func NewOneBlocksSource(
	lowestBlockNum uint64,
	store dstore.Store,
	handler Handler,
) (*oneBlocksSource, error) {

	ctx := context.Background()
	listCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	files, err := listOneBlocks(listCtx, lowestBlockNum, store)
	if err != nil {
		return nil, err
	}

	sourceCtx, cancel := context.WithCancel(ctx)

	src := &oneBlocksSource{
		oneBlockFiles: files,
		downloader:    OneBlockDownloaderFromStore(store),
		handler:       handler,
		ctx:           sourceCtx,
		Shutter: shutter.New(
			shutter.RegisterOnTerminating(func(_ error) {
				cancel()
			}),
		),
	}

	return src, nil
}

func (s *oneBlocksSource) Run() {
	s.Shutdown(s.run())
}

func (s *oneBlocksSource) run() error {
	for _, file := range s.oneBlockFiles {
		data, err := s.downloader(s.ctx, file)
		if err != nil {
			return err
		}

		reader := bytes.NewReader(data)
		blockReader, err := GetBlockReaderFactory.New(reader)
		if err != nil {
			return fmt.Errorf("unable to create block reader: %w", err)
		}
		blk, err := blockReader.Read()
		if err != nil && err != io.EOF {
			return fmt.Errorf("block reader failed: %w", err)
		}

		if err := s.handler.ProcessBlock(blk, nil); err != nil {
			return err
		}

	}
	zlog.Debug("one_blocks_source finish sending blocks", zap.Int("count", len(s.oneBlockFiles)))
	return nil
}

func listOneBlocks(ctx context.Context, from uint64, store dstore.Store) (out []*OneBlockFile, err error) {
	fromStr := fmt.Sprintf("%010d", from)
	err = store.WalkFrom(ctx, "", fromStr, func(filename string) error {
		obf, err := NewOneBlockFile(filename)
		if err != nil {
			return nil
		}
		out = append(out, obf)
		return nil
	})
	return
}
