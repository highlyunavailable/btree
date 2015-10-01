package btree

import (
	"fmt"
	"math/rand"
	"testing"
)

func drawChildren(level int, n node) {
	switch n.(type) {
	case *internalNode:
		for i := 0; i < level; i++ {
			fmt.Print("  - ")
		}
		fmt.Printf("Internal node %p: %+v\n", n, n)
		for _, k := range n.(*internalNode).keys {
			for i := 0; i < level; i++ {
				fmt.Print("  --")
			}
			fmt.Printf(" %+v\n", k)
		}
		for _, cn := range n.(*internalNode).nodes {
			drawChildren(level+1, cn)
		}
	case *leafNode:
		for i := 0; i < level; i++ {
			fmt.Print("  + ")
		}
		fmt.Printf("Leaf node %p: %+v\n", n, n)
		for _, k := range n.(*leafNode).keys {
			for i := 0; i < level; i++ {
				fmt.Print("  ++")
			}
			fmt.Printf(" %+v\n", k)
		}
	}
}

type testKey struct {
	value int
}

func (k *testKey) Less(other key) bool {
	o := other.(*testKey)
	return k.value < o.value
}

func (k *testKey) Compare(other key) int {
	o := other.(*testKey)
	if k.value == o.value {
		return 0
	}
	if k.value < o.value {
		return -1
	}
	return 1
}

func TestComparisonEquality(t *testing.T) {
	k1 := &testKey{value: 1}
	k2 := &testKey{value: 2}
	if !(k1.Less(k2) && k1.Compare(k2) == -1) {
		t.Fatalf("Failed to make sure less == true also equals compare == -1")
	}
}

func TestKeysInsertAt(t *testing.T) {
	ks := make(keys, 0, 15)
	ks = append(ks, &testKey{value: 1}, &testKey{value: 2}, &testKey{value: 4})

	if !(len(ks) == 3 && cap(ks) == 15) {
		t.Fatalf("Slice was of an unexpected size/capacity (%d/%d), expected %d/%d",
			len(ks), cap(ks), 3, 15)
	}
	ks.InsertAt(2, &testKey{value: 3})

	if !(len(ks) == 4 && cap(ks) == 15) {
		t.Fatalf("Slice was of an unexpected size/capacity (%d/%d), expected %d/%d",
			len(ks), cap(ks), 4, 15)
	}
}

func TestKeysSearchMissingEntry(t *testing.T) {
	ks := make(keys, 0, 15)
	ks = append(ks, &testKey{value: 1}, &testKey{value: 2}, &testKey{value: 4})

	index := ks.Search(&testKey{value: 3})

	if index != 2 {
		t.Fatalf("Search resulted in an unexpected slice index, got %d, expected %d",
			index, 2)
	}
	indexStart := ks.Search(&testKey{value: -1})

	if indexStart != 0 {
		t.Fatalf("Search resulted in an unexpected slice index, got %d, expected %d",
			indexStart, 0)
	}
	indexEnd := ks.Search(&testKey{value: 99})

	if indexEnd != len(ks) {
		t.Fatalf("Search resulted in an unexpected slice index, got %d, expected %d",
			indexEnd, len(ks))
	}
}

func TestKeysSearchExistingEntry(t *testing.T) {
	ks := make(keys, 0, 15)
	ks = append(ks, &testKey{value: 1}, &testKey{value: 2}, &testKey{value: 4})

	index := ks.Search(&testKey{value: 2})

	if index != 2 {
		t.Fatalf("Search resulted in an unexpected slice index, got %d, expected %d",
			index, 2)
	}

	indexNonContig := ks.Search(&testKey{value: 4})

	if indexNonContig != 3 {
		t.Fatalf("Search resulted in an unexpected slice index, got %d, expected %d",
			indexNonContig, 3)
	}
}

func TestLeafInsert(t *testing.T) {
	n := newLeafNode(16)
	for i := 0; i < 4; i++ {
		n.Insert(&testKey{value: i})
	}

	for i := 9; i >= 5; i-- {
		n.Insert(&testKey{value: i})
	}
	n.Insert(&testKey{value: 4})

	for i, k := range n.keys {
		if k.(*testKey).value != i {
			t.Fatalf("Got value %d instead of expected value %d at keys position %d", k.(*testKey).value, i, i)
		}
	}

	n.Insert(&testKey{value: -1})
	n.Insert(&testKey{value: 99})
	firstVal := n.keys[0].(*testKey).value
	lastVal := n.keys[len(n.keys)-1].(*testKey).value
	if firstVal != -1 {
		t.Fatalf("Got value %d instead of expected value %d at smallest key", firstVal, -1)
	}

	if lastVal != 99 {
		t.Fatalf("Got value %d instead of expected value %d at smallest key", lastVal, 99)
	}
}

func TestLeafSplit(t *testing.T) {
	n := newLeafNode(16)
	for i := 0; i < 16; i++ {
		n.Insert(&testKey{value: i})
	}

	pk, l, r := n.Split()

	if pk.(*testKey).value != 8 {
		t.Fatalf("Got value %d instead of expected value %d for split result key", pk.(*testKey).value, 8)
	}

	for i, k := range l.Keys() {
		if k.(*testKey).value != i {
			t.Fatalf("Got value %d instead of expected value %d left half keys position %d", k.(*testKey).value, i, i)
		}
	}
	for i, k := range r.Keys() {
		if k.(*testKey).value != i+8 {
			t.Fatalf("Got value %d instead of expected value %d right half keys position %d", k.(*testKey).value, i+8, i+8)
		}
	}
}

func TestLeafMergeRightIntoLeft(t *testing.T) {
	l := newLeafNode(16)
	for i := 0; i < 8; i++ {
		l.Insert(&testKey{value: i})
	}
	r := newLeafNode(16)
	for i := 8; i < 16; i++ {
		r.Insert(&testKey{value: i})
	}

	pk := l.Merge(r.Keys().First(), r)

	if pk.(*testKey).value != 0 {
		t.Fatalf("Got value %d instead of expected value %d for merge result key", pk.(*testKey).value, 0)
	}

	for i, k := range l.Keys() {
		if k.(*testKey).value != i {
			t.Fatalf("Got value %d instead of expected value %d merged keys position %d", k.(*testKey).value, i, i)
		}
	}
}

func TestLeafMergeLeftIntoRight(t *testing.T) {
	l := newLeafNode(16)
	for i := 0; i < 8; i++ {
		l.Insert(&testKey{value: i})
	}
	r := newLeafNode(16)
	for i := 8; i < 16; i++ {
		r.Insert(&testKey{value: i})
	}

	pk := r.Merge(nil, l)

	if pk.(*testKey).value != 0 {
		t.Fatalf("Got value %d instead of expected value %d for merge result key", pk.(*testKey).value, 0)
	}

	for i, k := range l.Keys() {
		if k.(*testKey).value != i {
			t.Fatalf("Got value %d instead of expected value %d merged keys position %d", k.(*testKey).value, i, i)
		}
	}
}

func TestLeafMergeSmall(t *testing.T) {
	l := newLeafNode(4)
	for i := 0; i < 2; i++ {
		l.Insert(&testKey{value: i})
	}
	r := newLeafNode(16)
	for i := 2; i < 4; i++ {
		r.Insert(&testKey{value: i})
	}

	pk := l.Merge(r.Keys().First(), r)

	if pk.(*testKey).value != 0 {
		t.Fatalf("Got value %d instead of expected value %d for merge result key", pk.(*testKey).value, 0)
	}

	for i, k := range l.Keys() {
		if k.(*testKey).value != i {
			t.Fatalf("Got value %d instead of expected value %d merged keys position %d", k.(*testKey).value, i, i)
		}
	}
}

func TestLeafRebalanceRightIntoLeft(t *testing.T) {
	l := newLeafNode(16)
	for i := 0; i < 8; i++ {
		l.Insert(&testKey{value: i})
	}
	r := newLeafNode(16)
	for i := 8; i < 24; i++ {
		r.Insert(&testKey{value: i})
	}

	//l.Rebalance(r.Keys().First(), r)

	//if leftKey.(*testKey).value != 0 {
	//  t.Fatalf("Got value %d instead of expected value %d for first left key", leftKey.(*testKey).value, 0)
	//}

	//for i, k := range l.Keys() {
	//  if k.(*testKey).value != i {
	//    t.Fatalf("Got value %d instead of expected value %d of left keys position %d", k.(*testKey).value, i, i)
	//  }
	//}

	//if rightKey.(*testKey).value != 12 {
	//  t.Fatalf("Got value %d instead of expected value %d for first right key", leftKey.(*testKey).value, 12)
	//}

	//for i, k := range r.Keys() {
	//  if k.(*testKey).value != i+12 {
	//    t.Fatalf("Got value %d instead of expected value %d of right keys position %d", k.(*testKey).value, i+12, i+12)
	//  }
	//}
}

func TestLeafRebalanceLeftIntoRight(t *testing.T) {
	l := newLeafNode(16)
	for i := 0; i < 8; i++ {
		l.Insert(&testKey{value: i})
	}
	r := newLeafNode(16)
	for i := 8; i < 24; i++ {
		r.Insert(&testKey{value: i})
	}

	//r.Rebalance(nil, l)

	//if leftKey.(*testKey).value != 0 {
	//  t.Fatalf("Got value %d instead of expected value %d for first left key", leftKey.(*testKey).value, 0)
	//}

	//for i, k := range l.Keys() {
	//  if k.(*testKey).value != i {
	//    t.Fatalf("Got value %d instead of expected value %d of left keys position %d", k.(*testKey).value, i, i)
	//  }
	//}

	//if rightKey.(*testKey).value != 12 {
	//  t.Fatalf("Got value %d instead of expected value %d for first right key", leftKey.(*testKey).value, 12)
	//}

	//for i, k := range r.Keys() {
	//  if k.(*testKey).value != i+12 {
	//    t.Fatalf("Got value %d instead of expected value %d of right keys position %d", k.(*testKey).value, i+12, i+12)
	//  }
	//}
}

func TestInternalCreate(t *testing.T) {
	n := newInternalNode(4)

	l := newLeafNode(4)
	for i := 0; i < 2; i++ {
		l.Insert(&testKey{value: i})
	}
	r := newLeafNode(4)
	for i := 2; i < 4; i++ {
		r.Insert(&testKey{value: i})
	}
	n.keys = append(n.keys, r.keys[0])
	n.nodes = append(n.nodes, l, r)
	rand := rand.New(rand.NewSource(99))

	for i := 1; i < 12; i++ {
		n.Insert(&testKey{value: rand.Intn(5000)})
	}
}

func TestTreeIter(t *testing.T) {
	tree := NewBTree(4)
	for i := 0; i < 16; i += 1 {
		tree.Insert(&testKey{value: i})
	}
	drawChildren(0, tree.root)
	tree.Remove(&testKey{value: 2})
	tree.Remove(&testKey{value: 1})
	tree.Remove(&testKey{value: 0})
	tree.Remove(&testKey{value: 5})
	tree.Remove(&testKey{value: 10})
	tree.Remove(&testKey{value: 15})
	tree.Remove(&testKey{value: 14})
	tree.Remove(&testKey{value: 4})
	tree.Remove(&testKey{value: 11})
	tree.Remove(&testKey{value: 3})
	tree.Remove(&testKey{value: 6})
	tree.Remove(&testKey{value: 12})
	//drawChildren(0, tree.root)
	//tree.Remove(&testKey{value: 52})
	//tree.Remove(&testKey{value: 56})
	//tree.Remove(&testKey{value: 60})
	//tree.Remove(&testKey{value: 48})
	//tree.Remove(&testKey{value: 36})
	fmt.Println("--------------------------------------------")
	drawChildren(0, tree.root)
	fmt.Println("--------------------------------------------")
}
