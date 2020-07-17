package forkable

import (
	"fmt"
	"sort"
	"strings"

	"github.com/dfuse-io/bstream"
)

type chain struct {
	nodes map[string]*node
}

func newChain() *chain {
	return &chain{
		nodes: map[string]*node{},
	}
}

func (c *chain) addLink(curRef bstream.BlockRef, obj interface{}, prevRef bstream.BlockRef) (exists bool) {
	curID := curRef.ID()
	cur := c.nodes[curID]
	if cur != nil {
		return true
	}

	var prev *node
	if prevRef != nil {
		prev = c.nodes[prevRef.ID()]
	}

	node := &node{
		ref:  curRef,
		obj:  obj,
		prev: prev,
	}

	if prev != nil {
		prev.nexts = append(prev.nexts, node)
	}

	c.nodes[curID] = node
	return false
}

func (c *chain) removeLink(ref bstream.BlockRef) {
	delete(c.nodes, ref.ID())
}

func (c *chain) getNode(ref bstream.BlockRef) *node {
	if ref == nil {
		return nil
	}

	return c.nodes[ref.ID()]
}

func (c *chain) nodesBetween(from, to *node) (out []*node) {
	var nodes []*node
	for {
		if to == nil {
			// We reached end of links, nothing more to do here
			break
		}

		if nodeEquals(from, to) {
			// Let's include the from node since it's not added yet
			if from != nil {
				nodes = append(nodes, from)
			}
			break
		}

		nodes = append(nodes, to)
		to = to.prev
	}

	// We skip the first element since its the `to` value wich is not included, so if we have 0 or 1 element, we return nothing
	if len(nodes) <= 1 {
		return nil
	}

	// Since we skip the first one and we have two elements, the sorted result is simply the second element
	if len(nodes) == 2 {
		return nodes[1:]
	}

	// Reverse but skip the first one (at index `0` on `nodes` array), hence `nodes[nodeCount-i]` instead of `nodes[nodeCount-i-1]`
	nodeCount := len(nodes) - 1
	out = make([]*node, nodeCount)
	for i := 0; i < nodeCount; i++ {
		out[i] = nodes[nodeCount-i]
	}

	return out
}

func (c *chain) debugNodes() (out []string) {
	i := 0
	nodes := make([]*node, len(c.nodes))
	for _, node := range c.nodes {
		nodes[i] = node
		i++
	}

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Num() == nodes[j].Num() {
			return nodes[i].ID() < nodes[j].ID()
		}

		return nodes[i].Num() < nodes[j].Num()
	})

	out = make([]string, len(nodes))
	for i, node := range nodes {
		out[i] = node.debug()
	}
	return
}

type node struct {
	ref bstream.BlockRef
	obj interface{}

	prev  *node
	nexts []*node
}

func (n *node) ID() string {
	return n.ref.ID()
}

func (n *node) Num() uint64 {
	return n.ref.Num()
}

func (n *node) String() string {
	if n == nil {
		return "Block <empty>"
	}

	return n.ref.String()
}

func (n *node) debug() string {
	if n == nil {
		return "<nil>"
	}

	prev := "<None>"
	if n.prev != nil {
		prev = n.prev.ref.String()
	}

	to := "<Empty>"
	if len(n.nexts) > 0 {
		tos := make([]string, len(n.nexts))
		for i, next := range n.nexts {
			tos[i] = next.ref.String()
		}

		to = strings.Join(tos, ", ")
	}

	return fmt.Sprintf("Prev (%s) --> Node (%s) --> To(s) [%s]", prev, n.ref.String(), to)
}

func nodeEquals(left *node, right *node) bool {
	if left == nil && right == nil {
		return true
	}

	if left == nil || right == nil {
		return false
	}

	return bstream.EqualsBlockRefs(left, right)
}
