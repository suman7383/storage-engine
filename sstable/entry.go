package sstable

import (
	"encoding/binary"

	"github.com/suman7383/storage-engine/internalkey"
)

// Entry respresents the binary layout used for disk storage
//
// ----  Binary Layout ----
//
// | KeyLen   |
// | ValueLen |
// | InternalKey      |
// | Value    |
type Entry struct {
	KeyLen      uint32 // 4 bytes
	ValueLen    uint32 // 4 bytes
	InternalKey []byte
	Value       []byte
}

// Returns a Entry with the given arguments
func NewEntry(internalKey internalkey.InternalKey, value []byte) *Entry {
	return &Entry{
		KeyLen:      uint32(len(internalKey)),
		ValueLen:    uint32(len(value)),
		InternalKey: internalKey,
		Value:       value,
	}
}

// EncodeEntry encodes to the binary layout given by the Entry struct
func EncodeEntry(internalKey internalkey.InternalKey, value []byte) []byte {
	keyLen, valueLen := len(internalKey), len(value)

	// InternalKeyLen bytes + valueLen bytes + KeyLen(4 bytes) + ValueLen(4 bytes)
	ebLen := keyLen + valueLen + 4 + 4

	eb := make([]byte, ebLen)
	offset := 0

	// KeyLen
	binary.LittleEndian.PutUint32(eb[offset:offset+4], uint32(keyLen))
	offset += 4

	// ValueLen
	binary.LittleEndian.PutUint32(eb[offset:offset+4], uint32(valueLen))
	offset += 4

	// Key
	copy(eb[offset:offset+keyLen], internalKey)
	offset += keyLen

	// Value
	copy(eb[offset:offset+valueLen], value)
	offset += valueLen

	return eb
}
