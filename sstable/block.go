package sstable

import (
	"encoding/binary"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
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
//
// It returns the value if present. If not found, it returns nil, false, false
// If the value is a tombstone, it returns nil, true, true
func (b *Block) linearSearch(key internalkey.InternalKey) (value []byte, isPresent bool, isTombstone bool) {
	offset := 0

	for offset < len(b.data) {
		keyLen := binary.LittleEndian.Uint32(b.data[offset : offset+4])
		offset += 4

		valueLen := binary.LittleEndian.Uint32(b.data[offset : offset+4])
		offset += 4

		ik := internalkey.InternalKey(b.data[offset : offset+int(keyLen)])
		offset += int(keyLen)

		// log.Printf("[SST READER] linear search comparing internalKey: %v", string(ik.UserKey()))

		if ik.EqualUserKeys(key) {
			if ik.IsPut() {
				value = make([]byte, valueLen)
				copy(value, b.data[offset:offset+int(valueLen)])

				return value, true, false
			}

			return nil, true, true
		}

		offset += int(valueLen)
	}

	return nil, false, false
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

// This is used in tests to create a block with key-value pairs for testing purposes.
type data struct {
	Key   []byte
	Value []byte
}

// Helper function to write a block with key-value pair
// for testing purposes.
// Binary layout:
// | KeyLen  			| 4 bytes
// | ValueLen 			| 4 bytes
// | InternalKey      	|
// | Value    			|
func writeBlock(data []data) *Block {
	var blockData []byte

	for i, d := range data {
		ik := internalkey.NewInternalKey(d.Key, uint64(i), op.OpPut) // sequence number is i, op is 0 (put)
		b := EncodeEntry(ik, d.Value)

		blockData = append(blockData, b...)
	}

	return &Block{data: blockData}
}
