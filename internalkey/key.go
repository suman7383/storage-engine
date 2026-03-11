package internalkey

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/suman7383/storage-engine/op"
)

// | 	userKey 			|
// | 	trailer (seq+kind)  | last 8 bytes
type InternalKey []byte

func NewInternalKey(userKey []byte, seq uint64, kind op.OpType) InternalKey {
	trailer := (seq << 8) | uint64(kind)

	buf := make([]byte, len(userKey)+8)
	copy(buf, userKey)

	binary.LittleEndian.PutUint64(buf[len(userKey):], trailer)

	return buf
}

func (i InternalKey) Seq() uint64 {
	trailer := ExtractTrailer(i)

	return (trailer >> 8)
}

func (i InternalKey) UserKey() []byte {
	return i[:len(i)-8]
}

func (i InternalKey) IsLessThan(to InternalKey) bool {
	cmp := i.Compare(to)

	return cmp == -1
}

func (i InternalKey) IsGreaterThan(to InternalKey) bool {
	cmp := i.Compare(to)

	return cmp == 1
}

// Checks if both the userKeys are equal
func (i InternalKey) EqualUserKeys(to InternalKey) bool {
	return CompareUserKeys(i, to) == 0
}

func (i InternalKey) IsDelete() bool {
	return extractKind(i) == op.OpDelete
}

func (i InternalKey) IsPut() bool {
	return extractKind(i) == op.OpPut
}

func extractKind(i InternalKey) op.OpType {
	trailerStart := len(i) - 8
	trailer, err := decodeUint64(i[trailerStart:])
	if err != nil {
		panic(err)
	}

	kind := trailer & 0xff
	return op.OpType(kind)
}

var ErrInvalidByteSize = errors.New("invalid byte slice")

func decodeUint64(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, ErrInvalidByteSize
	}

	return binary.LittleEndian.Uint64(b), nil
}

// Compare compares the "to" key with the key this method is called on.
// It returns 0 if both are equal, -1 if i < to and +1 if i > to
func (i InternalKey) Compare(to InternalKey) int {
	return CompareInternalKeys(i, to)
}

func CompareUserKeys(a, b InternalKey) int {
	aLen := len(a)
	bLen := len(b)

	userKeyA := a[:aLen-8]
	userKeyB := b[:bLen-8]

	return bytes.Compare(userKeyA, userKeyB)
}

func CompareInternalKeys(a, b InternalKey) int {

	if len(a) < 8 || len(b) < 8 {
		panic("invalid internal key")
	}

	cmpUserKeys := CompareUserKeys(a, b)
	if cmpUserKeys != 0 {
		return cmpUserKeys
	}

	trailerA := ExtractTrailer(a)
	trailerB := ExtractTrailer(b)

	if trailerA > trailerB {
		return -1 // because Seq and kind DESC
	}

	if trailerA < trailerB {
		return 1
	}

	return 0
}

func ExtractTrailer(i InternalKey) uint64 {
	if len(i) < 8 {
		panic("invalid internal key")
	}
	return binary.LittleEndian.Uint64(i[len(i)-8:])
}

func MakeInternalLookupKey(userKey []byte, snapshotSeq uint64) InternalKey {
	return NewInternalKey(userKey, snapshotSeq, 0xFF)
}

// This is used by WAL decoding
type Key struct {
	userKey []byte // provided by user
	seq     uint64 // sequence no
	kind    op.OpType
}

func NewKey(key []byte, seq uint64, kind op.OpType) Key {
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
	return k.kind == op.OpDelete
}

func (k Key) IsPut() bool {
	return k.kind == op.OpPut
}

func (k Key) Seq() uint64 {
	return k.seq
}

func (k Key) UserKey() []byte {
	return k.userKey
}

func Lookupkey(userKey []byte) Key {
	return NewKey(userKey, math.MaxUint64, op.OpPut)
}
