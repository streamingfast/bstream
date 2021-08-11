package forkable

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/opaque"
	pbbstream "github.com/streamingfast/pbgo/dfuse/bstream/v1"
)

type Cursor struct {
	Step  StepType
	Block bstream.BlockRef
	LIB   bstream.BlockRef // last block sent as irreversible if it exists, else known forkdb LIB

	// HeadBlock will be the same as Block when you receive a 'new' Step, except during a reorg.
	// During a reorg, (steps in ['new','redo','undo']) the HeadBlock will always point to the block that causes the reorg.
	// When the LIB is advancing (ex: DPOSLibNum changes, etc.), step='irreversible' and the HeadBlock will be the block
	// that causes previous blocks to become irreversible.
	HeadBlock bstream.BlockRef
}

var EmptyCursor = &Cursor{
	Block:     bstream.BlockRefEmpty,
	HeadBlock: bstream.BlockRefEmpty,
	LIB:       bstream.BlockRefEmpty,
}

func (c *Cursor) ToProto() pbbstream.Cursor {
	out := pbbstream.Cursor{
		Block: &pbbstream.BlockRef{
			Num: c.Block.Num(),
			Id:  c.Block.ID(),
		},
		HeadBlock: &pbbstream.BlockRef{
			Num: c.HeadBlock.Num(),
			Id:  c.HeadBlock.ID(),
		},
		Lib: &pbbstream.BlockRef{
			Num: c.LIB.Num(),
			Id:  c.LIB.ID(),
		},
	}
	switch c.Step {
	case StepNew, StepRedo:
		out.Step = pbbstream.ForkStep_STEP_NEW
	case StepUndo:
		out.Step = pbbstream.ForkStep_STEP_UNDO
	case StepIrreversible:
		out.Step = pbbstream.ForkStep_STEP_IRREVERSIBLE
	}
	return out
}

func (c *Cursor) ToOpaque() string {
	return opaque.EncodeString(c.String())
}

func CursorFromProto(in *pbbstream.Cursor) *Cursor {
	out := &Cursor{
		Block:     bstream.NewBlockRef(in.Block.Id, in.Block.Num),
		HeadBlock: bstream.NewBlockRef(in.HeadBlock.Id, in.HeadBlock.Num),
		LIB:       bstream.NewBlockRef(in.Lib.Id, in.Lib.Num),
	}
	switch in.Step {
	case pbbstream.ForkStep_STEP_NEW:
		out.Step = StepNew
	case pbbstream.ForkStep_STEP_UNDO:
		out.Step = StepUndo
	case pbbstream.ForkStep_STEP_IRREVERSIBLE:
		out.Step = StepIrreversible
	}
	return out
}

func CursorFromOpaque(in string) (*Cursor, error) {
	payload, err := opaque.DecodeToString(in)
	if err != nil {
		return nil, fmt.Errorf("unable to decode: %w", err)
	}
	return CursorFromString(payload)
}

func (c *Cursor) Equals(cc *Cursor) bool {
	if c.IsEmpty() {
		return cc.IsEmpty()
	}
	return c.Block.ID() == cc.Block.ID() &&
		c.HeadBlock.ID() == cc.HeadBlock.ID() &&
		c.LIB.ID() == cc.LIB.ID()
}

func (c *Cursor) IsEmpty() bool {
	return c == nil ||
		c.Block == nil ||
		c.Block.ID() == "" ||
		c.HeadBlock == nil ||
		c.HeadBlock.ID() == "" ||
		c.LIB == nil ||
		c.LIB.ID() == ""
}

// StartBlockNum gives you the info on "where to start feeding blocks"
func (c *Cursor) StartBlockNum() uint64 {
	if c.IsEmpty() {
		return 0
	}
	return c.LIB.Num()
}

func (c *Cursor) String() string {
	blkID := c.Block.ID()
	headID := c.HeadBlock.ID()
	libID := c.LIB.ID()
	if headID == blkID {
		return fmt.Sprintf("c1:%d:%d:%s:%d:%s", c.Step, c.Block.Num(), blkID, c.LIB.Num(), libID)
	}
	if blkID == libID {
		return fmt.Sprintf("c2:%d:%d:%s:%d:%s", c.Step, c.Block.Num(), blkID, c.HeadBlock.Num(), headID)
	}
	return fmt.Sprintf("c3:%d:%d:%s:%d:%s:%d:%s", c.Step, c.Block.Num(), blkID, c.HeadBlock.Num(), headID, c.LIB.Num(), libID)
}

func CursorFromString(cur string) (*Cursor, error) {
	parts := strings.Split(cur, ":")
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid cursor: too short")
	}

	switch parts[0] {
	case "c1":
		if len(parts) != 6 {
			return nil, fmt.Errorf("invalid cursor: invalid number of segments")
		}

		step, err := readCursorStep(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid step segment: %w", err)
		}

		blkRef, err := readCursorBlockRef(parts[2], parts[3])
		if err != nil {
			return nil, fmt.Errorf("invalid block ref segments: %w", err)
		}

		libRef, err := readCursorBlockRef(parts[4], parts[5])
		if err != nil {
			return nil, fmt.Errorf("invalid block ref segments: %w", err)
		}

		return &Cursor{
			Step:      step,
			Block:     blkRef,
			HeadBlock: blkRef,
			LIB:       libRef,
		}, nil

	case "c2":
		if len(parts) != 6 {
			return nil, fmt.Errorf("invalid cursor: invalid number of segments")
		}

		step, err := readCursorStep(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid step segment: %w", err)
		}

		blkRef, err := readCursorBlockRef(parts[2], parts[3])
		if err != nil {
			return nil, fmt.Errorf("invalid block ref segments: %w", err)
		}

		headBlkRef, err := readCursorBlockRef(parts[4], parts[5])
		if err != nil {
			return nil, fmt.Errorf("invalid head block ref segments: %w", err)
		}

		return &Cursor{
			Step:      step,
			Block:     blkRef,
			HeadBlock: headBlkRef,
			LIB:       blkRef,
		}, nil

	case "c3":
		if len(parts) != 8 {
			return nil, fmt.Errorf("invalid cursor: invalid number of segments")
		}

		step, err := readCursorStep(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid step segment: %w", err)
		}

		blkRef, err := readCursorBlockRef(parts[2], parts[3])
		if err != nil {
			return nil, fmt.Errorf("invalid block ref segments: %w", err)
		}

		headBlkRef, err := readCursorBlockRef(parts[4], parts[5])
		if err != nil {
			return nil, fmt.Errorf("invalid head block ref segments: %w", err)
		}

		libRef, err := readCursorBlockRef(parts[6], parts[7])
		if err != nil {
			return nil, fmt.Errorf("invalid block ref segments: %w", err)
		}

		return &Cursor{
			Step:      step,
			Block:     blkRef,
			HeadBlock: headBlkRef,
			LIB:       libRef,
		}, nil

	default:
		return nil, fmt.Errorf("invalid cursor: invalid prefix")
	}

}

func readCursorBlockRef(numStr string, id string) (bstream.BlockRef, error) {
	num, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block num: %w", err)
	}

	return bstream.NewBlockRef(id, num), nil
}

func readCursorStep(part string) (StepType, error) {
	step, err := strconv.ParseInt(part, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid cursor step: %w", err)
	}
	out := StepType(step)

	if !out.IsSingleStep() {
		return 0, fmt.Errorf("invalid step: %d", step)
	}
	return out, nil
}
