package internalkey

import (
	"bytes"
	"math"
)

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
