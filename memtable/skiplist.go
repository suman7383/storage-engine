package memtable

import (
	"bytes"
	"math/rand"
	"time"
)

type Node struct {
	key   []byte
	value []byte // nil for header/upper levels. Data for bottom level

	next *Node
	prev *Node
	up   *Node
	down *Node
}

type Skiplist struct {
	head   *Node   // Top-left header node (entry point for searches)
	height int     // Current number of levels in the skiplist
	size   int     // Total unique keys
	p      float64 // Promotional probability (0.5 typically)

	// for deterministic testing
	rand *rand.Rand // random number generator for coin flips
}

func NewSkipList() *Skiplist {
	return &Skiplist{
		head:   &Node{}, // initial header
		height: 1,
		size:   0,
		p:      0.5,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *Skiplist) Search(key []byte) *Node {
	// top-left header
	p := s.head

	for {
		p = p.down

		// Scan forward while key >= p.next.key
		for p.next != nil && bytes.Compare(key, p.next.key) >= 0 {
			p = p.next
		}

		// Try to go down
		if p.down == nil {
			break // At the bottom level
		}
		p = p.down
	}

	return p
}
