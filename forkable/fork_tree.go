package forkable

import (
	"sort"
)

type ForkableError string

func (e ForkableError) Error() string { return string(e) }

const RootNotFound = ForkableError("root not found")
const NoLinkErr = ForkableError("no link")

func (db *ForkDB) BuildTree() (*Node, error) {
	db.linksLock.Lock()
	defer db.linksLock.Unlock()

	root, err := db.root()
	if err != nil {
		return nil, err
	}
	tree := db.buildTreeWithID(root)

	return tree, nil
}

func (db *ForkDB) root() (string, error) {
	if db.libID == "" {
		return "", RootNotFound
	}

	next := db.libID
	root := ""
	for {
		parent, found := db.links[next]
		if found {
			root = next
			next = parent
			continue
		}
		break
	}
	if root == "" {
		return "", RootNotFound
	}

	return root, nil
}

func (n *Node) Chains() *ChainList {
	chains := &ChainList{
		Chains: [][]string{},
	}
	n.chains(nil, chains)

	return chains
}

func (db *ForkDB) BuildTreeWithID(root string) *Node {
	db.linksLock.Lock()
	defer db.linksLock.Unlock()

	return db.buildTreeWithID(root)
}
func (db *ForkDB) buildTreeWithID(root string) *Node {
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

type Node struct {
	ID       string
	Children []*Node
}

func newNode(id string) *Node {
	return &Node{
		ID: id,
	}
}

func (n *Node) growBranches(db *ForkDB) {
	children := db.findChildren(n.ID)

	for _, childID := range children {
		node := newNode(childID)
		node.growBranches(db)
		n.Children = append(n.Children, node)
	}
}
func (n *Node) chains(current []string, out *ChainList) {
	current = append(current, n.ID)
	if len(n.Children) == 0 { //reach the leaf
		out.Chains = append(out.Chains, current)
		return
	}

	for _, child := range n.Children {
		c := make([]string, len(current))
		copy(c, current)
		child.chains(c, out)
	}
}

func (n *Node) Size() int {
	if len(n.Children) == 0 {
		return 0
	}

	return n.size(0)
}

func (n *Node) size(count int) int {
	for _, child := range n.Children {
		count = child.size(count)
	}
	count++
	return count
}

type ChainList struct {
	Chains [][]string
}

func (l *ChainList) LongestChain() []string {
	count := 0
	longestID := -1
	longestLen := 0
	for i, chain := range l.Chains {
		if len(chain) == longestLen {
			count++
		}
		if len(chain) > longestLen {
			count = 1
			longestLen = len(chain)
			longestID = i
		}
	}

	if count > 1 { // found multiple chain with same length
		return nil
	}

	if len(l.Chains) > 0 {
		return l.Chains[longestID]
	}

	return nil
}

func (db *ForkDB) Roots() ([]string, error) {
	if len(db.links) == 0 {
		return nil, NoLinkErr
	}
	roots := db.roots()

	return roots, nil
}

func (db *ForkDB) roots() []string {
	var roots []string
	for blockID, prevID := range db.links {
		if _, found := db.links[prevID]; !found {
			roots = append(roots, blockID)
		}
	}
	sort.Strings(roots)
	return roots
}
