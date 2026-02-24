package memtable

import (
	"math/bits"
	"math/rand"
	"time"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/snapshot"
)

const MAX_LEVEL = 16

// Node represents a node in the skiplist.
// We use RocksDB style skipList node which ensures lock-free readers
//
// next[] is the entire skiplist
//
// next[0]  → next node in full list
// next[1]  → skip further
// next[2]  → skip even further
//
// e.g:
// Level 2:  10 --------------------► 50
// Level 1:  10 -------► 30 --------► 50
// Level 0:  10 ─► 20 ─► 30 ─► 40 ─► 50
//
// would look like:
// Node 10:
//
//	next[0] → 20
//	next[1] → 30
//	next[2] → 50
//
// Node 30:
//
//	next[0] → 40
//	next[1] → 50
type Node struct {
	Key   internalkey.InternalKey
	Value []byte // nil for header/upper levels. Data for bottom level

	height int     // number of levels this node spans
	next   []*Node // forward pointers, one per level
}

func NewNode(key internalkey.InternalKey, value []byte, height int) *Node {
	n := &Node{
		Key:    key,
		Value:  value,
		height: height,
		next:   make([]*Node, height),
	}

	return n
}

type Skiplist struct {
	head      *Node // special sentinel node (entry point for searches)
	maxHeight int   // Current max eight in the skiplist
	size      int   // Total unique keys
	// p         float64 // Promotional probability (0.5 typically)

	// for deterministic testing
	rand *rand.Rand // random number generator for coin flips
}

func NewSkipList() *Skiplist {
	head := &Node{
		height: MAX_LEVEL,
		next:   make([]*Node, MAX_LEVEL),
	}

	for i := 0; i < MAX_LEVEL; i++ {
		head.next[i] = nil
	}

	sl := &Skiplist{
		head:      head, // initial header
		maxHeight: 1,
		size:      0,
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	return sl
}

func (s *Skiplist) Search(key internalkey.InternalKey) (*Node, bool) {
	x := s.head
	level := s.maxHeight - 1

	for level >= 0 {
		for x.next[level] != nil && x.next[level].Key.Compare(key) < 0 {
			x = x.next[level]
		}

		level -= 1
	}

	x = x.next[0]

	if x == nil || !x.Key.EqualUserKeys(key) || x.Key.IsDelete() {
		return nil, false
	}

	if x.Key.IsPut() {
		return x, true
	}

	return nil, false
}

func (s *Skiplist) SearchWithSnapshot(key internalkey.InternalKey, snapshot snapshot.Snapshot) (*Node, bool) {
	x := s.head
	level := s.maxHeight - 1

	for level >= 0 {
		for x.next[level] != nil && x.next[level].Key.Compare(key) < 0 {
			x = x.next[level]
		}

		level -= 1
	}

	x = x.next[0]

	for x != nil {

		if !x.Key.EqualUserKeys(key) {
			return nil, false
		}

		if x.Key.Seq() > snapshot.Seq() {
			x = x.next[0]
			continue
		}

		if x.Key.IsDelete() {
			return nil, false
		}

		if x.Key.IsPut() {
			return x, true
		}
	}

	return nil, false
}

// Insert inserts the key, value to the skiplist and returns the height of the newNode
// iserted
func (s *Skiplist) Insert(key internalkey.InternalKey, value []byte) int {
	update := make([]*Node, MAX_LEVEL)
	x := s.head

	for level := s.maxHeight - 1; level >= 0; level-- {
		for x.next[level] != nil && x.next[level].Key.Compare(key) < 0 {
			x = x.next[level]
		}
		update[level] = x
	}

	nodeHeight := s.randomHeight()
	newNode := NewNode(key, value, nodeHeight)

	if nodeHeight > s.maxHeight {
		for i := s.maxHeight; i < nodeHeight; i++ {
			update[i] = s.head
		}
		s.maxHeight = nodeHeight
	}

	for level := 0; level < nodeHeight; level++ {
		newNode.next[level] = update[level].next[level]
		update[level].next[level] = newNode
	}

	s.size++
	return newNode.height
}

func (s *Skiplist) randomHeight() int {
	// Generate a random 64-bit number
	r := s.rand.Uint64()

	// Count trailing zero bits
	// Probability of k trailing zeros is 1 / 2^(k+1)
	height := bits.TrailingZeros64(r) + 1

	if height > MAX_LEVEL {
		height = MAX_LEVEL
	}

	return height
}

// func (s *Skiplist) coinFlip() bool {
// 	return s.rand.Float64() < s.p
// }
