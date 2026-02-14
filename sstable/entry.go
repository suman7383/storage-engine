package sstable

import "encoding/binary"

// Entry respresents the binary layout used for disk storage
//
// ----  Binary Layout ----
//
// | KeyLen   |
// | ValueLen |
// | Seq      |
// | Kind     |
// | Key      |
// | Value    |
type Entry struct {
	KeyLen   uint32 // 4 bytes
	ValueLen uint32 // 4 bytes
	Seq      uint64 // 8 bytes
	Kind     uint8  // 1 byte
	Key      []byte
	Value    []byte
}

// Returns a Entry with the given arguments
func NewEntry(seq uint64, kind uint8, key, value []byte) *Entry {
	return &Entry{
		KeyLen:   uint32(len(key)),
		ValueLen: uint32(len(value)),
		Seq:      seq,
		Key:      key,
		Value:    value,
		Kind:     kind,
	}
}

// EncodeEntry encodes to the binary layout given by the Entry struct
func EncodeEntry(key, value []byte, seq uint64, kind uint8) []byte {
	keyLen, valueLen := len(key), len(value)

	// keyLen bytes + valueLen bytes + KeyLen(4 bytes) + ValueLen(4 bytes) + Seq(8 bytes)
	// + Kind(1 byte)
	ebLen := keyLen + valueLen + 4 + 4 + 8 + 1

	eb := make([]byte, ebLen)
	offset := 0

	// KeyLen
	binary.LittleEndian.PutUint32(eb[offset:offset+4], uint32(keyLen))
	offset += 4

	// ValueLen
	binary.LittleEndian.PutUint32(eb[offset:offset+4], uint32(valueLen))
	offset += 4

	// Seq
	binary.LittleEndian.PutUint64(eb[offset:offset+8], seq)
	offset += 8

	// Kind
	eb[offset] = kind
	offset += 1

	// Key
	copy(eb[offset:offset+keyLen], key)
	offset += keyLen

	// Value
	copy(eb[offset:offset+valueLen], value)
	offset += valueLen

	return eb
}
