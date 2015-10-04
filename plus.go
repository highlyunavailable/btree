package btree

import "sort"

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
	if i == len(*ks) {
		*ks = append(*ks, k)
	} else {
		*ks = append(*ks, nil)
		copy((*ks)[i+1:], (*ks)[i:])
		(*ks)[i] = k
	}
}

func (ks *keys) RemoveAt(i int) {
	*ks = append((*ks)[:i], (*ks)[i+1:]...)
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
	if i == len(*ns) {
		*ns = append(*ns, n)
	} else {
		*ns = append(*ns, nil)
		copy((*ns)[i+1:], (*ns)[i:])
		(*ns)[i] = n
	}
}

func (ns *nodes) RemoveAt(i int) {
	*ns = append((*ns)[:i], (*ns)[i+1:]...)
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
	switch idx := n.Search(k); {
	case idx == len(n.keys):
		return idx - 1, len(n.nodes) - 1
	case k.Less(n.keys[idx]):
		return idx, idx
	default:
		return idx, idx + 1
	}
}

func (n *internalNode) Insert(k key) {
	kIdx, nIdx := n.searchKNIndex(k)

	child := n.nodes[nIdx]

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

	child.Insert(k)
}

func (n *internalNode) Remove(k key) {
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
		switch {
		case curNIdx == len(n.nodes)-1: // At the end
			if child.CanMerge(n.nodes[curNIdx-1]) { // Merge case:
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
					n.keys[curKIdx] = n.nodes[curNIdx].Keys().First()
				case *internalNode:
				}
			} else {
				n.keys[curKIdx] = child.RebalanceToHead(n.nodes[curNIdx-1])
			}
		case curKIdx == 0 && curNIdx == 0: // At the beginning
			if child.CanMerge(n.nodes[1]) { // Merge case:
				n.nodes[1].Merge(n.keys[0], n.nodes[0])
				n.nodes.RemoveAt(0)
				if len(n.keys) > 1 {
					n.keys.RemoveAt(0)
				}
			} else {
				n.keys[curKIdx] = child.RebalanceToTail(n.nodes[curNIdx+1])
			}
		default: // In the middle
			if child.CanMerge(n.nodes[curNIdx+1]) { // Merge case:
				child.Merge(n.keys[curKIdx], n.nodes[curNIdx+1])
				n.nodes.RemoveAt(curNIdx + 1)
				// Don't delete the last key
				if len(n.keys) > 1 {
					n.keys.RemoveAt(curKIdx + 1)
				}

				switch n.nodes[curNIdx].(type) {
				case *leafNode:
					n.keys[curKIdx] = n.nodes[curNIdx].Keys().First()
				case *internalNode:
					nextLowest := n.nodes[curNIdx].GetLowestLeaf()
					n.keys[curKIdx] = nextLowest
				}
			} else {
				n.keys[curKIdx] = child.RebalanceToTail(n.nodes[curNIdx+1])
			}
		}
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
	// drop 1 key and return it.
	keyRight := mn.keys[moveIdx-1]
	n.keys = append(append(n.keys, mn.GetLowestLeaf()), mn.keys[:moveIdx-1]...)
	n.nodes = append(n.nodes, mn.nodes[:moveIdx]...)

	mn.keys = append(mn.keys[:0], mn.keys[moveIdx:]...)
	mn.nodes = append(mn.nodes[:0], mn.nodes[moveIdx:]...)
	return keyRight
}

// Rebalances to the head of this node, removing items from the tail of other.
func (n *internalNode) RebalanceToHead(other node) key {
	mn := other.(*internalNode)
	moveIdx := len(mn.keys) - (len(mn.keys) - cap(mn.keys)/2)
	keyLeft := mn.keys[moveIdx-1]
	n.keys = append(n.keys[:0], append(append(mn.keys[moveIdx:], n.GetLowestLeaf()), n.keys...)...)
	n.nodes = append(n.nodes[:0], append(mn.nodes[moveIdx:], n.nodes...)...)

	mn.keys = append(mn.keys[:0], mn.keys[:moveIdx-1]...)
	mn.nodes = append(mn.nodes[:0], mn.nodes[:moveIdx]...)
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
	keyRight := mn.keys[moveIdx]
	n.keys = append(n.keys, mn.keys[:moveIdx]...)

	mn.keys = append(mn.keys[:0], mn.keys[moveIdx:]...)
	return keyRight
}

// Rebalances to the head of this node, removing items from the tail of other.
func (n *leafNode) RebalanceToHead(other node) key {
	mn := other.(*leafNode)
	moveIdx := len(mn.keys) - (len(mn.keys) - cap(mn.keys)/2)
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
