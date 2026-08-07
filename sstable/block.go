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
