package memtable

import (
	"errors"
	"unsafe"

	"github.com/suman7383/storage-engine/internalkey"
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

// Apply takes a record and appends it to the memtable.
//
// It returns the seq number of the appended record
// and error if any.
// Apply is not concurrent safe. The caller must ensure this before using this
func (m *Memtable) Apply(userKey, value []byte, seq uint64) (uint64, error) {
	if m.isFrozen {
		return 0, ErrMemtableFrozen
	}
	ik := internalkey.NewInternalKey(userKey, seq, internalkey.KeyPut)

	h := m.skl.Insert(ik, value)

	m.approxSize += computeSize(h, userKey, value)

	return seq, nil
}

// searches the skiplist and returns the node
// ok is true if found else it returns nil, false
//
// Get always returns the latest version of the key
func (m *Memtable) Get(userKey []byte, snapshotSeq uint64) (rec *Node, ok bool) {
	lk := internalkey.MakeInternalLookupKey(userKey, snapshotSeq)

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

// computeSize calculates the size of the rec added to memtable
func computeSize(nodeHeight int, key, value []byte) uint64 {
	baseSize := unsafe.Sizeof(Node{})

	pointerArraySize := uintptr(nodeHeight) * unsafe.Sizeof((*Node)(nil))

	keyBytes := len(key)

	valueBytes := len(value)

	return uint64(baseSize) + uint64(pointerArraySize) + uint64(keyBytes) + uint64(valueBytes)
}
