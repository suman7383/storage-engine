package internalkey

import (
	"bytes"
	"encoding/binary"
	"math"
)

// | 	userKey 			|
// | 	trailer (seq+kind)  | last 8 bytes
type InternalKey []byte

func NewInternalKey(userKey []byte, seq uint64, kind uint8) InternalKey {
	trailer := (seq << 8) | uint64(kind)

	buf := make([]byte, len(userKey)+8)
	copy(buf, userKey)

	binary.LittleEndian.PutUint64(buf[len(userKey):], trailer)

	return buf
}

func (i InternalKey) Compare(to InternalKey) int {
	return CompareInternalKeys(i, to)
}

func CompareInternalKeys(a, b InternalKey) int {
	aLen := len(a)
	bLen := len(b)

	if aLen < 8 || bLen < 8 {
		panic("invalid internal key")
	}

	userKeyA := a[:aLen-8]
	userKeyB := b[:bLen-8]

	cmp := bytes.Compare(userKeyA, userKeyB)
	if cmp != 0 {
		return cmp
	}

	trailerA := binary.LittleEndian.Uint64(a[aLen-8:])
	trailerB := binary.LittleEndian.Uint64(b[bLen-8:])

	if trailerA > trailerB {
		return -1 // because Seq and kind DESC
	}

	if trailerA < trailerB {
		return 1
	}

	return 0
}

func MakeInternalLookupKey(userKey []byte, snapshotSeq uint64) InternalKey {
	return NewInternalKey(userKey, snapshotSeq, 0xFF)
}

// This is used by WAL decoding
type Key struct {
	userKey []byte // provided by user
	seq     uint64 // sequence no
	kind    KeyType
}

type KeyType uint8

const (
	KeyPut    KeyType = 0
	KeyDelete KeyType = 1
)

func NewKey(key []byte, seq uint64, kind KeyType) Key {
	return Key{
		userKey: key,
		seq:     seq,
		kind:    kind,
	}
}

// Compares current "key" to "to" key
//
// compares userKey ASC
// then seq DESC
func (k Key) Compare(to Key) int {
	// Compare userKey (ASC)
	if c := bytes.Compare(k.userKey, to.userKey); c != 0 {
		return c
	}

	// Compare seq (DESC)
	if k.Seq() > to.Seq() {
		return -1
	}
	if k.Seq() < to.Seq() {
		return 1
	}

	// Compare kind (ASC)
	if k.kind < to.kind {
		return -1
	}
	if k.kind > to.kind {
		return 1
	}

	return 0
}

func (k Key) Equal(to Key) bool {
	return bytes.Equal(k.userKey, to.userKey)
}

func (k Key) IsDelete() bool {
	return k.kind == KeyDelete
}

func (k Key) IsPut() bool {
	return k.kind == KeyPut
}

func (k Key) Seq() uint64 {
	return k.seq
}

func (k Key) UserKey() []byte {
	return k.userKey
}

func Lookupkey(userKey []byte) Key {
	return NewKey(userKey, math.MaxUint64, KeyPut)
}
