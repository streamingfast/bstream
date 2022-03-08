package transform

import (
	"context"
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/dstore"
	pbbstream "github.com/streamingfast/pbgo/sf/bstream/v1"
)

// blockIndex is a generic index for existence of certain keys at certain block heights
type blockIndex struct {
	// kv is the main data structure to identify blocks of interest
	kv map[string]*roaring64.Bitmap

	// lowBlockNum is the lower bound of the current index
	lowBlockNum uint64

	// indexSize is the distance between upper and lower bounds of this BlockIndex
	// thus, the index's exclusive upper bound is determined with lowBlockNum + indexSize
	indexSize uint64
}

// NewBlockIndex initializes and returns a new BlockIndex
func NewBlockIndex(lowBlockNum, indexSize uint64) *blockIndex {
	return &blockIndex{
		lowBlockNum: lowBlockNum,
		indexSize:   indexSize,
		kv:          make(map[string]*roaring64.Bitmap),
	}
}

func (i *blockIndex) Get(key string) *roaring64.Bitmap {
	return i.kv[key]
}

func (i *blockIndex) contains(num uint64) bool {
	if i == nil {
		return false
	}
	return num > i.lowBlockNum && num < (i.lowBlockNum+i.indexSize)
}

// marshal converts the current index to a protocol buffer
func (i *blockIndex) marshal() ([]byte, error) {
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

// unmarshal converts a protocol buffer to the current index
func (i *blockIndex) unmarshal(in []byte) error {
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

// add will append the given blockNum to the bitmap identified by the given key
func (i *blockIndex) add(key string, blocknum uint64) {
	bitmap, ok := i.kv[key]
	if !ok {
		i.kv[key] = roaring64.BitmapOf(blocknum)
		return
	}
	bitmap.Add(blocknum)
}

// lazyBlockIndex contains info about an index, it can return a blockIndex from file or memoized value
type lazyBlockIndex struct {
	sync.Mutex
	lowBlockNum uint64
	indexSize   uint64

	blockIndex *blockIndex
	err        error
}

func newLazyBlockIndex(lowBlockNum, indexSize uint64) *lazyBlockIndex {
	return &lazyBlockIndex{
		lowBlockNum: lowBlockNum,
		indexSize:   indexSize,
	}
}

func (lbi *lazyBlockIndex) contains(num uint64) bool {
	if lbi == nil {
		return false
	}
	return num > lbi.lowBlockNum && num < (lbi.lowBlockNum+lbi.indexSize)
}

func (lbi *lazyBlockIndex) load(ctx context.Context, store dstore.Store, indexShortname string) (*blockIndex, error) {
	lbi.Lock()
	defer lbi.Unlock()
	if lbi.blockIndex != nil || lbi.err != nil {
		zlog.Debug("returning block index from lazy_block_index")
		return lbi.blockIndex, lbi.err
	}
	zlog.Debug("not from lazy..")

	filename := toIndexFilename(lbi.indexSize, lbi.lowBlockNum, indexShortname)
	r, err := store.OpenObject(ctx, filename)
	if err != nil {
		lbi.err = err
		return nil, err
	}

	obj, err := ioutil.ReadAll(r)
	if err != nil {
		lbi.err = err
		return nil, err
	}

	newIdx := NewBlockIndex(lbi.lowBlockNum, lbi.indexSize)
	err = newIdx.unmarshal(obj)
	if err != nil {
		lbi.err = err
		return nil, err
	}

	lbi.blockIndex = newIdx
	return newIdx, nil
}
