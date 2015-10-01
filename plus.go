package btree

import (
	"fmt"
	"sort"
)

type BTree struct {
	root  node
	order uint
}

func NewBTree(d uint) *BTree {
	return &BTree{
		root:  newLeafNode(d),
		order: d,
	}
}

func (t *BTree) Insert(k key) {
	t.root.Insert(k)
	if t.root.IsFull() {
		key, left, right := t.root.Split()

		r := newInternalNode(t.order)
		r.keys = append(r.keys, key)
		r.nodes = append(r.nodes, left, right)
		t.root = r
	}
}

func (t *BTree) Remove(k key) {
	t.root.Remove(k)
	if r, ok := t.root.(*internalNode); ok {
		if len(r.nodes) < 2 {
			//r.nodes[0].Merge(r.keys[0], r.nodes[1])
			t.root = r.nodes[0]
		}
	}
}

func (t *BTree) Get(k key) key {
	return t.root.Get(k)
}

type key interface {
	Less(key) bool
	Compare(key) int
}

type keys []key

func (ks keys) Len() int           { return len(ks) }
func (ks keys) Less(i, j int) bool { return ks[i].Less(ks[j]) }
func (ks keys) Swap(i, j int)      { ks[i], ks[j] = ks[j], ks[i] }
func (ks keys) Search(x key) int {
	return sort.Search(len(ks), func(i int) bool { return ks[i].Compare(x) >= 0 })
}

func (ks *keys) InsertAt(i int, k key) {
	keys := *ks
	if i == len(keys) {
		keys = append(keys, k)
	} else {
		keys = append(keys, nil)
		copy(keys[i+1:], keys[i:])
		keys[i] = k
	}
	*ks = keys
}

func (ks *keys) RemoveAt(i int) {
	keys := *ks
	keys = append(keys[:i], keys[i+1:]...)
	*ks = keys
}

func (ks keys) First() key {
	return ks[0]
}

func (ks keys) Last() key {
	return ks[len(ks)-1]
}

type node interface {
	Insert(key)
	Remove(key)

	Search(key) int
	Get(key) key
	GetLowestLeaf() key
	Keys() keys
	Less(node) bool

	Split() (key, node, node)
	Merge(key, node) key
	RebalanceToHead(node) key
	RebalanceToTail(node) key

	IsFull() bool
	IsEmpty() bool
	CanMerge(node) bool
}

type nodes []node

func (ns nodes) Len() int           { return len(ns) }
func (ns nodes) Less(i, j int) bool { return ns[i].Less(ns[j]) }
func (ns nodes) Swap(i, j int)      { ns[i], ns[j] = ns[j], ns[i] }

func (ns *nodes) InsertAt(i int, n node) {
	nodes := *ns

	if i == len(nodes) {
		nodes = append(nodes, n)
	} else {
		nodes = append(nodes, nil)
		copy(nodes[i+1:], nodes[i:])
		nodes[i] = n
	}
	*ns = nodes
}

func (ns *nodes) RemoveAt(i int) {
	nodes := *ns
	nodes = append(nodes[:i], nodes[i+1:]...)
	*ns = nodes
}

func (ns nodes) SplitAt(i int) (left, right nodes) {
	lslice, rslice := ns[:i], ns[i:]
	left = make(nodes, len(lslice), cap(ns))
	right = make(nodes, len(rslice), cap(ns))
	copy(left, lslice)
	copy(right, rslice)
	return
}

type internalNode struct {
	keys  keys
	nodes nodes
}

func newInternalNode(b uint) *internalNode {
	return &internalNode{
		keys:  make(keys, 0, b),
		nodes: make(nodes, 0, b+1),
	}
}

func (n *internalNode) searchKNIndex(k key) (int, int) {
	idx := n.Search(k)

	switch idx {
	case len(n.keys):
		if n.keys.Last().Less(k) {
			return idx - 1, len(n.nodes) - 1
		}
		return idx, idx
	case 0:
		if k.Less(n.keys[0]) {
			return 0, 0
		}
		return 0, 1
	default:
		if k.Less(n.keys[idx]) {
			return idx, idx
		}
		return idx, idx + 1
	}
}

func (n *internalNode) Insert(k key) {
	kIdx, nIdx := n.searchKNIndex(k)

	child := n.nodes[nIdx]

	child.Insert(k)
	if child.IsFull() {
		key, left, right := child.Split()
		n.keys.InsertAt(n.Search(key), key)
		n.nodes[nIdx] = left
		n.nodes.InsertAt(nIdx+1, right)
		if k.Less(key) {
			child = left
		} else {
			child = right
			kIdx++
		}
	}
}

func (n *internalNode) Remove(k key) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("!!! PANIC !!!")
			drawChildren(0, n)
			fmt.Println("!!! PANIC !!!")
			panic(r)
		}
	}()
	curKIdx, curNIdx := n.searchKNIndex(k)
	child := n.nodes[curNIdx]
	child.Remove(k)
	if k.Compare(n.keys[curKIdx]) == 0 {
		n.keys[curKIdx] = child.GetLowestLeaf()
	}
	if len(n.nodes) == 1 {
		return
	}
	if child.IsEmpty() {
		fmt.Println("Starting merge/rebalance check")
		drawChildren(0, n)
		switch {
		case curNIdx == len(n.nodes)-1: // At the end
			fmt.Println("end")
			if child.CanMerge(n.nodes[curNIdx-1]) { // Merge case:
				fmt.Println("merge")
				n.nodes[curNIdx-1].Merge(n.keys[curKIdx], child)
				n.nodes.RemoveAt(curNIdx)
				// Reduce the current node and key index since the trailing items were
				// just deleted
				curNIdx--
				// Don't delete the last key
				if len(n.keys) > 1 {
					n.keys.RemoveAt(curKIdx)

					curKIdx--
				}

				switch n.nodes[curNIdx].(type) {
				case *leafNode:
					fmt.Println("leaf", curKIdx)
					n.keys[curKIdx] = n.nodes[curNIdx].Keys().First()
				case *internalNode:
				}
			} else {
				fmt.Println("rebalance")
				n.keys[curKIdx] = child.RebalanceToHead(n.nodes[curNIdx-1])
			}
		case curKIdx == 0 && curNIdx == 0: // At the beginning
			fmt.Println("start")
			if child.CanMerge(n.nodes[1]) { // Merge case:
				fmt.Println("merge")
				n.nodes[1].Merge(n.keys[0], n.nodes[0])
				n.nodes.RemoveAt(0)
				if len(n.keys) > 1 {
					n.keys.RemoveAt(0)
				}
			} else {
				fmt.Println("rebalance")
				n.keys[curKIdx] = child.RebalanceToTail(n.nodes[curNIdx+1])
			}
		default: // In the middle
			fmt.Println("middle")
			if child.CanMerge(n.nodes[curNIdx+1]) { // Merge case:
				fmt.Println("merge")
				child.Merge(n.keys[curKIdx], n.nodes[curNIdx+1])
				n.nodes.RemoveAt(curNIdx + 1)
				// Don't delete the last key
				if len(n.keys) > 1 {
					n.keys.RemoveAt(curKIdx + 1)
				}

				switch n.nodes[curNIdx].(type) {
				case *leafNode:
					fmt.Println("leaf", curKIdx)
					n.keys[curKIdx] = n.nodes[curNIdx].Keys().First()
				case *internalNode:
					nextLowest := n.nodes[curNIdx].GetLowestLeaf()
					n.keys[curKIdx] = nextLowest
				}
			} else {
				fmt.Println("rebalance")
				n.keys[curKIdx] = child.RebalanceToTail(n.nodes[curNIdx+1])
			}
		}
		drawChildren(0, n)
		fmt.Println("finish remove")
	}
}

func (n *internalNode) Search(k key) int {
	return n.keys.Search(k)
}

func (n *internalNode) Get(k key) key {
	_, nIdx := n.searchKNIndex(k)
	return n.nodes[nIdx].Get(k)
}

func (n *internalNode) GetLowestLeaf() key {
	return n.nodes[0].GetLowestLeaf()
}

func (n *internalNode) Keys() keys {
	return n.keys
}

func (n *internalNode) Less(o node) bool {
	return n.keys.Last().Less(o.Keys().First())
}

func (n *internalNode) Split() (key, node, node) {
	if len(n.keys) < 3 {
		panic("Internal node too small to split")
	}

	mid := len(n.keys) / 2
	key := n.keys[mid]

	lslice, rslice := n.keys[:mid], n.keys[mid+1:]

	leftNodes, rightNodes := n.nodes.SplitAt(mid + 1)

	left := &internalNode{
		keys:  make(keys, len(lslice), cap(n.keys)),
		nodes: leftNodes,
	}

	rightSubset := make(keys, len(rslice), cap(n.keys))

	right := n

	copy(left.keys, lslice)
	copy(rightSubset, rslice)

	right.keys = rightSubset
	right.nodes = rightNodes
	return key, left, right
}

func (n *internalNode) Merge(parent key, toMerge node) key {
	mn := toMerge.(*internalNode)
	fmt.Println(parent, n.keys[0], mn.keys[0])
	if n.Less(mn) {
		if parent.Less(mn.keys[0]) {
			n.keys.InsertAt(n.keys.Search(parent), parent)
		}
		n.keys = append(n.keys, mn.keys...)
		n.nodes = append(n.nodes, mn.nodes...)
	} else {
		if parent.Less(n.keys[0]) {
			n.keys.InsertAt(n.keys.Search(parent), parent)
		}
		n.keys = append(n.keys[:0], append(mn.keys, n.keys...)...)
		n.nodes = append(n.nodes[:0], append(mn.nodes, n.nodes...)...)
	}
	return n.keys.First()
}

// Rebalances to the tail of this node, removing items from the head of other.
func (n *internalNode) RebalanceToTail(other node) key {
	mn := other.(*internalNode)
	moveIdx := len(mn.keys) - 1 - (len(mn.keys) - cap(mn.keys)/2)
	fmt.Println("in internal rebalance")
	// drop 1 key and return it.
	keyRight := mn.keys[moveIdx-1]
	fmt.Println("to tail", keyRight)
	n.keys = append(append(n.keys, mn.GetLowestLeaf()), mn.keys[:moveIdx-1]...)
	n.nodes = append(n.nodes, mn.nodes[:moveIdx]...)

	mn.keys = append(mn.keys[:0], mn.keys[moveIdx:]...)
	mn.nodes = append(mn.nodes[:0], mn.nodes[moveIdx:]...)
	fmt.Println(keyRight)
	return keyRight
}

// Rebalances to the head of this node, removing items from the tail of other.
func (n *internalNode) RebalanceToHead(other node) key {
	mn := other.(*internalNode)
	moveIdx := len(mn.keys) - (len(mn.keys) - cap(mn.keys)/2)
	fmt.Println("in internal rebalance")
	keyLeft := mn.keys[moveIdx-1]
	fmt.Println("to head", keyLeft)
	n.keys = append(n.keys[:0], append(append(mn.keys[moveIdx:], n.GetLowestLeaf()), n.keys...)...)
	n.nodes = append(n.nodes[:0], append(mn.nodes[moveIdx:], n.nodes...)...)

	mn.keys = append(mn.keys[:0], mn.keys[:moveIdx-1]...)
	mn.nodes = append(mn.nodes[:0], mn.nodes[:moveIdx]...)
	fmt.Println(keyLeft)
	return keyLeft
}

func (n *internalNode) IsFull() bool {
	return len(n.keys) == cap(n.keys)
}

func (n *internalNode) IsEmpty() bool {
	return len(n.keys) <= cap(n.keys)/2
}

func (n *internalNode) CanMerge(other node) bool {
	if o, ok := other.(*internalNode); ok {
		return len(n.keys)+len(o.keys) <= cap(n.keys) && len(n.nodes)+len(o.nodes) <= cap(n.nodes)
	} else {
		return false
	}
	fmt.Println("internal isEmpty: keys:", len(n.keys), "/", cap(n.keys),
		"nodes:", len(n.nodes), "/", cap(n.nodes),
		"empty", len(n.keys) <= cap(n.keys)/2)

	return len(n.keys) <= cap(n.keys)/2 && len(n.nodes) < cap(n.nodes)/2
}

type leafNode struct {
	keys           keys
	next, previous *leafNode
}

func newLeafNode(b uint) *leafNode {
	return &leafNode{
		keys: make(keys, 0, b),
	}
}

func (n *leafNode) Insert(k key) {
	n.keys.InsertAt(n.keys.Search(k), k)
}

func (n *leafNode) Remove(k key) {
	fmt.Println("remove leaf key", k)
	i := n.keys.Search(k)

	if i < len(n.keys) {
		if k.Compare(n.keys[i]) == 0 {
			n.keys.RemoveAt(i)
		}
	}
}

func (n *leafNode) Search(k key) int {
	return n.keys.Search(k)
}

func (n *leafNode) Get(k key) key {
	i := n.Search(k)
	if i == len(n.keys) {
		return nil
	}
	return n.keys[i]
}

func (n *leafNode) GetLowestLeaf() key {
	return n.keys[0]
}

func (n *leafNode) Keys() keys {
	return n.keys
}

func (n *leafNode) Less(o node) bool {
	return n.keys.Last().Less(o.Keys().First())
}

func (n *leafNode) Split() (key, node, node) {
	if len(n.keys) < 2 {
		panic("Leaf node too small to split")
	}

	mid := len(n.keys) / 2
	key := n.keys[mid]
	lslice, rslice := n.keys[:mid], n.keys[mid:]

	left := &leafNode{
		keys:     make(keys, len(lslice), cap(n.keys)),
		previous: n.previous,
		next:     n,
	}

	rightSubset := make(keys, len(rslice), cap(n.keys))

	right := n
	right.previous = left

	copy(left.keys, lslice)
	copy(rightSubset, rslice)

	right.keys = rightSubset
	return key, left, right
}

func (n *leafNode) Merge(parent key, toMerge node) key {
	mn := toMerge.(*leafNode)
	if n.Less(mn) {
		n.keys = append(n.keys, mn.keys...)
	} else {
		n.keys = append(mn.keys, n.keys...)
	}
	return n.keys.First()
}

// Rebalances to the tail of this node, removing items from the head of other.
func (n *leafNode) RebalanceToTail(other node) key {
	mn := other.(*leafNode)
	moveIdx := len(mn.keys) - 1 - (len(mn.keys) - cap(mn.keys)/2)
	fmt.Println("in leaf rebalance")
	fmt.Println("to tail")
	keyRight := mn.keys[moveIdx]
	n.keys = append(n.keys, mn.keys[:moveIdx]...)

	mn.keys = append(mn.keys[:0], mn.keys[moveIdx:]...)
	return keyRight
}

// Rebalances to the head of this node, removing items from the tail of other.
func (n *leafNode) RebalanceToHead(other node) key {
	mn := other.(*leafNode)
	moveIdx := len(mn.keys) - (len(mn.keys) - cap(mn.keys)/2)
	fmt.Println("in leaf rebalance")
	fmt.Println("to head")
	keyLeft := mn.keys[moveIdx]

	n.keys = append(n.keys[:0], append(mn.keys[moveIdx:], n.keys...)...)

	mn.keys = append(mn.keys[:0], mn.keys[:moveIdx]...)
	return keyLeft
}

func (n *leafNode) IsFull() bool {
	return len(n.keys) == cap(n.keys)
}

func (n *leafNode) IsEmpty() bool {
	return len(n.keys) <= cap(n.keys)/2
}

func (n *leafNode) CanMerge(other node) bool {
	if o, ok := other.(*leafNode); ok {
		return len(n.keys)+len(o.keys) <= cap(n.keys)
	} else {
		return false
	}
}
