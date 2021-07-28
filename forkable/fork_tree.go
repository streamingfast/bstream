package forkable

import (
	"fmt"
	"sort"
)

type node struct {
	id       string
	children []*node
}

type chainList struct {
	chains [][]string
}

func newNode(id string) *node {
	return &node{
		id: id,
	}
}

func (n *node) growBranches(db *ForkDB) {
	children := db.findChildren(n.id)

	for _, childID := range children {
		node := newNode(childID)
		node.growBranches(db)
		n.children = append(n.children, node)
	}
}
func (n *node) chains(current []string, out *chainList) {
	current = append(current, n.id)
	if len(n.children) == 0 { //reach the leaf
		out.chains = append(out.chains, current)
		return
	}

	for _, child := range n.children {
		c := make([]string, len(current))
		copy(c, current)
		child.chains(c, out)
	}
}

func (n *node) LongestChain() ([]string, error) {
	chains := &chainList{
		chains: [][]string{},
	}
	n.chains(nil, chains)

	var out []string
	for _, chain := range chains.chains {
		if len(chain) > len(out) {
			out = chain
		}
	}

	return out, nil
}

//ForkDB addons
func (db *ForkDB) BuildTree() (*node, error) {
	db.linksLock.Lock()
	defer db.linksLock.Unlock()

	root, err := db.root()
	if err != nil {
		return nil, err
	}
	return db.buildTreeWithID(root), nil
}

func (db *ForkDB) BuildTreeWithID(root string) *node {
	db.linksLock.Lock()
	defer db.linksLock.Unlock()

	return db.buildTreeWithID(root)
}
func (db *ForkDB) buildTreeWithID(root string) *node {
	rootNode := newNode(root)
	rootNode.growBranches(db)
	return rootNode
}

func (db *ForkDB) findChildren(parentID string) []string {
	var children []string
	for id, prevID := range db.links {
		if prevID == parentID {
			children = append(children, id)
		}
	}
	sort.Strings(children)
	return children
}
func (db *ForkDB) roots() []string {
	var roots []string
	for blockID, prevID := range db.links {
		if _, found := db.links[prevID]; !found {
			roots = append(roots, blockID)
		}
	}
	return roots
}

func (db *ForkDB) root() (string, error) {
	if len(db.links) == 0 {
		return "", fmt.Errorf("no link")
	}
	roots := db.roots()

	if len(roots) > 1 {
		return "", fmt.Errorf("multiple root found: %d", len(roots))
	}
	return roots[0], nil
}
