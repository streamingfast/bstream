package bstream

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/peterbourgon/diskv"
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

var GetBlockCacheDir = "/data"

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

var payloadDiskv *diskv.Diskv

func init() {
	InitCache(GetBlockCacheDir)
}

func InitCache(basePath string) {
	cachePath := path.Join(basePath, "diskv")
	flatTransform := func(s string) []string { return []string{} }
	payloadDiskv = diskv.New(diskv.Options{
		BasePath:     cachePath,
		Transform:    flatTransform,
		CacheSizeMax: 20_000,
		Index:        &diskv.BTreeIndex{},
		IndexLess:    func(a, b string) bool { return a < b },
	})
}

type DiskCachedBlockPayload struct {
	cacheKey string
}

func (p DiskCachedBlockPayload) Get() (data []byte, err error) {
	//todo: if block not found just reload the right merge file.
	//todo: add cache weight on bstream.Block
	data, err = payloadDiskv.Read(p.cacheKey)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		panic("missing data for: " + p.cacheKey)
	}

	return
}

func DiskCachedPayloadSetter(block *Block, data []byte) (*Block, error) {
	err := payloadDiskv.Write(block.Id, data)
	if err != nil {
		return nil, err
	}

	block.Payload = &DiskCachedBlockPayload{
		cacheKey: block.Id,
	}

	return block, err
}
