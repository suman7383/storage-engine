package memtable

import (
	"errors"
	"fmt"
	"log"
	"unsafe"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
	"github.com/suman7383/storage-engine/snapshot"
)

type Memtable struct {
	skl        *Skiplist
	approxSize uint64
	isFrozen   bool
}

func NewMemtable(skl *Skiplist) *Memtable {
	return &Memtable{
		skl:        skl,
		approxSize: 0,
		isFrozen:   false,
	}
}

var ErrApplyingToMemtable = errors.New("inserting into skiplist returned false")
var ErrMemtableFrozen = errors.New("memtable is frozen and in read-only state. Cannot append to frozen memtable")

// Apply either performs put or delete operation with the given options depending
// upon the operation passed(KeyType)
func (m *Memtable) Apply(userKey, value []byte, seq uint64, operation op.OpType) (uint64, error) {

	switch operation {
	case op.OpPut:
		return m.Put(userKey, value, seq)
	case op.OpDelete:
		return m.Delete(userKey, seq)
	default:
		return 0, fmt.Errorf("Invalid operation on memtable: %v", operation)
	}
}

// Put takes a record and appends it to the memtable.
//
// It returns the seq number of the appended record
// and error if any.
// Apply is not concurrent safe. The caller must ensure this before using this
func (m *Memtable) Put(userKey, value []byte, seq uint64) (uint64, error) {
	if m.isFrozen {
		return 0, ErrMemtableFrozen
	}
	ik := internalkey.NewInternalKey(userKey, seq, op.OpPut)

	err := m.apply(ik, userKey, value)

	return seq, err
}

func (m *Memtable) Delete(userKey []byte, seq uint64) (uint64, error) {
	if m.isFrozen {
		return 0, ErrMemtableFrozen
	}
	ik := internalkey.NewInternalKey(userKey, seq, op.OpDelete)

	err := m.apply(ik, userKey, nil)

	return seq, err
}

func (m *Memtable) apply(ik internalkey.InternalKey, userKey []byte, value []byte) error {
	h := m.skl.Insert(ik, value)

	m.approxSize += computeSize(h, userKey, value)

	return nil
}

// searches the skiplist and returns the node
// ok is true if found else it returns nil, false
//
// Get always returns the latest version of the key
func (m *Memtable) Get(userKey []byte, snapshotSeq uint64) (rec *Node, ok bool) {
	lk := internalkey.MakeInternalLookupKey(userKey, snapshotSeq)

	log.Printf("[MEMTABLE] internalKey size: %v", len(lk))

	return m.skl.Search(lk)
}

// searches the skiplist useing the snapshot to return the latest version <= snapshot.
// It returns node and ok is true if found else it returns nil, false.
//
// GetWithSnapshot returns the record w.r.t userKey whose seq no <= snapshot's seq no.
// It is safe for concurrent reads
func (m *Memtable) GetWithSnapshot(userKey []byte, snapshot snapshot.Snapshot) (rec *Node, ok bool) {
	lk := internalkey.MakeInternalLookupKey(userKey, snapshot.Seq())

	return m.skl.SearchWithSnapshot(lk, snapshot)
}

// Frozen checks and returns whether the memtable is in frozen state(i.e isFrozen is true)
func (m *Memtable) Frozen() bool {
	return m.isFrozen
}

// Freeze freezes the current memtable, sets the isFrozen field
// to true and return the latest value of the field
func (m *Memtable) Freeze() (ok bool) {
	m.isFrozen = true

	return m.isFrozen
}

// Size returns the current approxSize of the memtable
func (m *Memtable) Size() uint64 {
	return m.approxSize
}

func (m *Memtable) NewIterator() *SkiplistIterator {
	return m.skl.NewIterator()
}

// computeSize calculates the size of the rec added to memtable
func computeSize(nodeHeight int, key, value []byte) uint64 {
	baseSize := unsafe.Sizeof(Node{})

	pointerArraySize := uintptr(nodeHeight) * unsafe.Sizeof((*Node)(nil))

	keyBytes := len(key)

	valueBytes := len(value)

	return uint64(baseSize) + uint64(pointerArraySize) + uint64(keyBytes) + uint64(valueBytes)
}
