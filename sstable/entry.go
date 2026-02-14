package sstable

// Entry respresents the binary layout used for disk storage
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
