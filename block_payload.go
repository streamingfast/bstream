package bstream

import (
	"fmt"
	"io/ioutil"
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

func FileBlockPayloadSetter(block *Block, data []byte) (*Block, error) {
	file, err := ioutil.TempFile("", block.ID())
	if err != nil {
		return nil, fmt.Errorf("creating payload temp file for block: %d %s: %w", block.Num(), block.ID(), err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return nil, fmt.Errorf("writing payload temp file for block: %d %s: %w", block.Num(), block.ID(), err)
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
