package transform

import (
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoaring_SaveLoad(t *testing.T) {
	t.Skip() // this was learning example
	r := roaring64.NewBitmap()
	r.Add(1000)
	r.Add(2000)
	r.Add(2005)
	r.Add(20000005)
	r.Add(530000005)

	short, err := r.ToBase64()
	require.NoError(t, err)

	bts, err := r.ToBytes()
	require.NoError(t, err)

	r2 := roaring64.NewBitmap()
	r2.UnmarshalBinary(bts)

	short2, err := r2.ToBase64()
	require.NoError(t, err)

	assert.Equal(t, short, short2)
}

func TestBlockIndex(t *testing.T) {

	tests := []struct {
		name string
		in   *BlockIndex
	}{
		{
			name: "sunny",
			in: &BlockIndex{
				kv: map[string]*roaring64.Bitmap{
					"bob":   roaring64.BitmapOf(1, 2, 3),
					"alice": roaring64.BitmapOf(4, 5),
				},
			},
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {

			data, err := c.in.Marshal()
			require.NoError(t, err)

			out := &BlockIndex{}
			err = out.Unmarshal(data)
			require.NoError(t, err)

			for k, v := range c.in.kv {
				outVal, ok := out.kv[k]
				require.True(t, ok)
				assert.Equal(t, v.ToArray(), outVal.ToArray())
			}
		})
	}
}

func TestBlockIndexAdd(t *testing.T) {

	type kvsingle struct {
		key string
		num uint64
	}
	type kv struct {
		key  string
		nums []uint64
	}

	tests := []struct {
		name         string
		add          []kvsingle
		expectedKeys []kv
	}{
		{
			name: "sunny",
			add: []kvsingle{
				{"alice", 10},
				{"alice", 12},
				{"bob", 11},
				{"alice", 14},
				{"colin", 20},
			},
			expectedKeys: []kv{
				{"alice", []uint64{10, 12, 14}},
				{"bob", []uint64{11}},
				{"colin", []uint64{20}},
			},
		},
	}

	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			bi := NewBlockIndex(999, 999)

			for _, i := range c.add {
				bi.Add(i.key, i.num)
			}

			for _, i := range c.expectedKeys {
				seen, ok := bi.KV()[i.key]
				require.True(t, ok)
				assert.Equal(t, i.nums, seen.ToArray())
			}
		})
	}
}
