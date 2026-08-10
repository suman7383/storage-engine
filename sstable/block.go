package sstable

import (
	"encoding/binary"

	"github.com/suman7383/storage-engine/internalkey"
)

type Block struct {
	data []byte

	// TODO: Add fields for block metadata
}

// ----  Binary Layout ----
//
// | KeyLen  			| 4 bytes
// | ValueLen 			| 4 bytes
// | InternalKey      	|
// | Value    			|
func (b *Block) linearSearch(key internalkey.InternalKey) (value []byte, ok bool) {
	offset := 0

	for offset < len(b.data) {
		keyLen := binary.LittleEndian.Uint32(b.data[offset : offset+4])
		offset += 4

		valueLen := binary.LittleEndian.Uint32(b.data[offset : offset+4])
		offset += 4

		ik := internalkey.InternalKey(b.data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		// log.Printf("[SST READER] linear search comparing internalKey: %v", string(ik.UserKey()))

		if ik.EqualUserKeys(key) && ik.IsPut() {
			value = make([]byte, valueLen)
			copy(value, b.data[offset:offset+int(valueLen)])

			return value, true
		}

		offset += int(valueLen)
	}

	return nil, false
}

func (b *Block) NewIterator() *BlockIterator {
	return NewBlockIterator(b)
}

// MoveToNextEntry moves the offset to the next entry in the block and returns the next key/value pair.
// It returns the new offset, the key, the value, and a boolean indicating if the next entry exists.
func (b *Block) MoveToNextEntry(offset int) (nextOffset int, key internalkey.InternalKey, value []byte, ok bool) {
	// Check if offset is valid
	if offset >= len(b.data) || offset < 0 || len(b.data[offset:]) < 8 {
		return 0, internalkey.InternalKey(nil), nil, false
	}

	// Read key length
	keyLen := binary.LittleEndian.Uint32(b.data[offset : offset+4])
	offset += 4

	// Read value length
	valueLen := binary.LittleEndian.Uint32(b.data[offset : offset+4])
	offset += 4

	// Check if there is enough data for the key and value
	if remaining := len(b.data) - offset; remaining < int(keyLen+valueLen) {
		return 0, internalkey.InternalKey(nil), nil, false
	}

	// Read key
	keyBytes := make([]byte, keyLen)
	copy(keyBytes, b.data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Read value
	value = make([]byte, valueLen)
	copy(value, b.data[offset:offset+int(valueLen)])
	offset += int(valueLen)

	return offset, internalkey.InternalKey(keyBytes), value, true
}
