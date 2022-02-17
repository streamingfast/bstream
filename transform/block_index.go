package transform

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/golang/protobuf/proto"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
)

// BlockIndex is a generic index for existence of certain keys at certain block heights
type BlockIndex struct {
	kv          map[string]*roaring64.Bitmap
	lowBlockNum uint64
	indexSize   uint64
}

func NewBlockIndex(lowBlockNum, indexSize uint64) *BlockIndex {
	return &BlockIndex{
		lowBlockNum: lowBlockNum,
		indexSize:   indexSize,
		kv:          make(map[string]*roaring64.Bitmap),
	}
}

func (i *BlockIndex) Marshal() ([]byte, error) {
	pbIndex := &pbbstream.GenericBlockIndex{}

	for k, v := range i.kv {
		bitmapBytes, err := v.ToBytes()
		if err != nil {
			return nil, err
		}

		pbIndex.Kv = append(pbIndex.Kv, &pbbstream.KeyToBitmap{
			Key:    []byte(k),
			Bitmap: bitmapBytes,
		})
	}

	return proto.Marshal(pbIndex)
}

func (i *BlockIndex) Unmarshal(in []byte) error {
	pbIndex := &pbbstream.GenericBlockIndex{}
	if i.kv == nil {
		i.kv = make(map[string]*roaring64.Bitmap)
	}

	if err := proto.Unmarshal(in, pbIndex); err != nil {
		return fmt.Errorf("couldn't unmarshal GenericBlockIndex: %s", err)
	}

	for _, data := range pbIndex.Kv {
		key := string(data.Key)

		r64 := roaring64.NewBitmap()
		err := r64.UnmarshalBinary(data.Bitmap)
		if err != nil {
			return fmt.Errorf("coudln't unmarshal kv bitmap: %s", err)
		}

		i.kv[key] = r64
	}
	return nil
}

func (i *BlockIndex) KV() map[string]*roaring64.Bitmap {
	return i.kv
}

func (i *BlockIndex) add(key string, blocknum uint64) {
	bitmap, ok := i.kv[key]
	if !ok {
		i.kv[key] = roaring64.BitmapOf(blocknum)
		return
	}
	bitmap.Add(blocknum)
}
