package transform

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/golang/protobuf/proto"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
)

// BlockIndex is a generic index for existence of certain keys at certain block heights
type BlockIndex struct {
	// kv is the main data structure to identify blocks of interest
	kv map[string]*roaring64.Bitmap

	// lowBlockNum is the lower bound of the current index
	lowBlockNum uint64

	// indexSize is the distance between upper and lower bounds of this BlockIndex
	// thus, the index's exclusive upper bound is determined with lowBlockNum + indexSize
	indexSize uint64
}

// NewBlockIndex initializes and returns a new BlockIndex
func NewBlockIndex(lowBlockNum, indexSize uint64) *BlockIndex {
	return &BlockIndex{
		lowBlockNum: lowBlockNum,
		indexSize:   indexSize,
		kv:          make(map[string]*roaring64.Bitmap),
	}
}

// KV returns the contents of the current index
func (i *BlockIndex) KV() map[string]*roaring64.Bitmap {
	return i.kv
}

// LowBlockNum returns the block number at the lower bound of the current index
func (i *BlockIndex) LowBlockNum() uint64 {
	return i.lowBlockNum
}

// IndexSize returns the size of the current index
// thus, the index's exclusive upper bound is determined with LowBlockNum + IndexSize
func (i *BlockIndex) IndexSize() uint64 {
	return i.indexSize
}

// Marshal converts the current index to a protocol buffer
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

// Unmarshal converts a protocol buffer to the current index
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

// Add will append the given blockNum to the bitmap identified by the given key
func (i *BlockIndex) Add(key string, blocknum uint64) {
	bitmap, ok := i.kv[key]
	if !ok {
		i.kv[key] = roaring64.BitmapOf(blocknum)
		return
	}
	bitmap.Add(blocknum)
}
