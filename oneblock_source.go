package bstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
)

type oneBlocksSource struct {
	*shutter.Shutter
	oneBlockFiles      []*OneBlockFile
	downloader         OneBlockDownloaderFunc
	handler            Handler
	blockReaderFactory BlockReaderFactory
	ctx                context.Context
}

func NewOneBlocksSource(
	lowestBlockNum uint64,
	targetHeadID string,
	store dstore.Store,
	handler Handler,
	blockReaderFactory BlockReaderFactory,
) (*oneBlocksSource, error) {

	ctx := context.Background()
	listCtx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	files, err := listOneBlocks(listCtx, lowestBlockNum, store)
	if err != nil {
		return nil, err
	}

	orderedFiles := walkUpChain(files, TruncateBlockID(targetHeadID))
	if err != nil {
		return nil, err
	}

	sourceCtx, cancel := context.WithCancel(ctx)

	src := &oneBlocksSource{
		oneBlockFiles:      orderedFiles,
		downloader:         OneBlockDownloaderFromStore(store),
		blockReaderFactory: blockReaderFactory,
		handler:            handler,
		ctx:                sourceCtx,
		Shutter: shutter.New(
			shutter.RegisterOnTerminating(func(_ error) {
				cancel()
			}),
		),
	}

	return src, nil
}

func (s *oneBlocksSource) Run() error {
	for _, file := range s.oneBlockFiles {

		data, err := file.Data(s.ctx, s.downloader)
		if err != nil {
			return err
		}

		reader := bytes.NewReader(data)
		blockReader, err := s.blockReaderFactory.New(reader)
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
	return nil
}

func walkUpChain(files map[string]*OneBlockFile, headID string) (out []*OneBlockFile) {
	nextID := headID
	for {
		if obf := files[nextID]; obf != nil {
			out = append(out, obf)
			nextID = obf.PreviousID
		} else {
			sort.Slice(out, func(i, j int) bool {
				return out[i].Num < out[j].Num
			})
			return
		}
	}
}

func listOneBlocks(ctx context.Context, from uint64, store dstore.Store) (out map[string]*OneBlockFile, err error) {
	fromStr := fmt.Sprintf("%010d", from)
	out = make(map[string]*OneBlockFile)
	err = store.WalkFrom(ctx, "", fromStr, func(filename string) error {
		obf, err := NewOneBlockFile(filename)
		if err != nil {
			return nil
		}
		out[obf.ID] = obf
		return nil
	})
	return
}
