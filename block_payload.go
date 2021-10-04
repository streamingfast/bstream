package bstream

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/streamingfast/atm"
)

var GetBlockPayloadSetter BlockPayloadSetter

type BlockPayloadSetter func(block *Block, data []byte) (*Block, error)

type BlockPayload interface {
	Get() (data []byte, err error)
}

type MemoryBlockPayload struct {
	data []byte
}

func MemoryBlockPayloadSetter(block *Block, data []byte) (*Block, error) {
	block.Payload = &MemoryBlockPayload{
		data: data,
	}

	return block, nil
}

func (p *MemoryBlockPayload) Get() (data []byte, err error) {
	return p.data, err
}

type FileBlockPayload struct {
	file string
}

var GetBlockCacheDir = "/tmp"

func FileBlockPayloadSetter(block *Block, data []byte) (*Block, error) {
	file, err := os.Create(filepath.Join(GetBlockCacheDir, block.ID()))
	if err != nil {
		return nil, fmt.Errorf("creating payload file for block: %d %s: %w", block.Num(), block.ID(), err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return nil, fmt.Errorf("writing payload file for block: %d %s: %w", block.Num(), block.ID(), err)
	}

	block.Payload = &FileBlockPayload{
		file: file.Name(),
	}
	return block, err
}

func (p *FileBlockPayload) Get() (data []byte, err error) {
	data, err = ioutil.ReadFile(p.file)
	if err != nil {
		return nil, fmt.Errorf("reading payload data from temp file: %s: %w", p.file, err)
	}
	return
}

var atmCache *atm.Cache

func getCache() *atm.Cache {
	if atmCache == nil {
		initCache(GetBlockCacheDir)
	}
	return atmCache
}

func initCache(basePath string) {
	cachePath := path.Join(basePath, "atm")
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		err := os.Mkdir(cachePath, os.ModePerm)
		if err != nil {
			panic(err)
		}
	}
	var err error
	atmCache, err = atm.NewInitializedCache(cachePath, 21474836480, 21474836480, atm.NewFileIO())
	if err != nil {
		panic(fmt.Sprintf("failed to initialize cache: %s: %s", cachePath, err))
	}
}

type DiskCachedBlockPayload struct {
	cacheKey string
	dataSize int
}

func (p DiskCachedBlockPayload) Get() (data []byte, err error) {
	//todo: if block not found just reload the right merge file.
	//todo: add cache weight on bstream.Block
	data, err = getCache().Read(p.cacheKey)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		panic("missing data for: " + p.cacheKey)
	}

	return
}

func DiskCachedPayloadSetter(block *Block, data []byte) (*Block, error) {
	_, err := getCache().Write(block.Id, block.Timestamp, data)
	if err != nil {
		return nil, err
	}

	block.Payload = &DiskCachedBlockPayload{
		cacheKey: block.Id,
		dataSize: len(data),
	}

	return block, err
}
