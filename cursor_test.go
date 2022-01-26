package bstream

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromString(t *testing.T) {
	ref := func(num uint64, id string) BlockRef {
		return NewBlockRef(id, num)
	}
	emptyRef := ref(0, "")

	tests := []struct {
		name        string
		in          string
		expected    *Cursor
		expectedErr error
	}{
		{
			"c1 no LIB",
			"c1:1:11846516:d46eeb12ad30ef291a673329bb2f64bc689f15253371b64aaee017556ee95a35:0:",
			&Cursor{
				Step:      StepNew,
				Block:     ref(11846516, "d46eeb12ad30ef291a673329bb2f64bc689f15253371b64aaee017556ee95a35"),
				HeadBlock: ref(11846516, "d46eeb12ad30ef291a673329bb2f64bc689f15253371b64aaee017556ee95a35"),
				LIB:       emptyRef,
			},
			nil,
		},
		{
			"c2 full",
			"c2:1:7393903:e9e04d1f639ffd8491fd3c90153b341e68a2ef9aaa72337dc926d928384f8f71:7393905:4c01ca1daced994d7a87faa92a14a360a1b2f64340d97e82b579915765c36663",
			&Cursor{
				Step:      StepNew,
				Block:     ref(7393903, "e9e04d1f639ffd8491fd3c90153b341e68a2ef9aaa72337dc926d928384f8f71"),
				HeadBlock: ref(7393905, "4c01ca1daced994d7a87faa92a14a360a1b2f64340d97e82b579915765c36663"),
				LIB:       ref(7393903, "e9e04d1f639ffd8491fd3c90153b341e68a2ef9aaa72337dc926d928384f8f71"),
			},
			nil,
		},

		{
			"c3 full",
			"c3:1:7393903:e9e04d1f639ffd8491fd3c90153b341e68a2ef9aaa72337dc926d928384f8f71:7393905:4c01ca1daced994d7a87faa92a14a360a1b2f64340d97e82b579915765c36663:7393704:fc119c952209a330f6276f98cff168e4cd14f6edd34505e8d67a5e929d48d93a",
			&Cursor{
				Step:      StepNew,
				Block:     ref(7393903, "e9e04d1f639ffd8491fd3c90153b341e68a2ef9aaa72337dc926d928384f8f71"),
				HeadBlock: ref(7393905, "4c01ca1daced994d7a87faa92a14a360a1b2f64340d97e82b579915765c36663"),
				LIB:       ref(7393704, "fc119c952209a330f6276f98cff168e4cd14f6edd34505e8d67a5e929d48d93a"),
			},
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := FromString(test.in)
			if test.expectedErr == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expected, actual)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
		})
	}
}
