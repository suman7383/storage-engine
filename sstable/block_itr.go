package sstable

// BlockIterator is an iterator for traversing key/value pairs in a Block.
type BlockIterator struct {
	block *Block
	// offset is the current position in the block data.
	offset int

	currentKey   []byte
	currentValue []byte

	// valid indicates whether the iterator is currently pointing to a valid key/value pair.
	// Initially, the iterator is not valid until Next() is called for the first time.
	valid bool
}

func NewBlockIterator(block *Block) *BlockIterator {
	return &BlockIterator{
		block:  block,
		offset: 0,
	}
}

// Next moves the iterator to the next key/value pair in the block.
// It returns true if there is a next key/value pair,
// and false if the iterator has reached the end of the block.
func (bi *BlockIterator) Next() bool {
	nextOffset, key, value, ok := bi.block.MoveToNextEntry(bi.offset)
	// If there is no next entry, return false
	if !ok {
		bi.valid = false
		return false
	}

	bi.offset = nextOffset
	bi.currentKey = key
	bi.currentValue = value
	bi.valid = true

	return true
}

// Key returns the current key in the block iterator.
func (bi *BlockIterator) Key() []byte {
	return bi.currentKey
}

// Value returns the current value in the block iterator.
func (bi *BlockIterator) Value() []byte {
	return bi.currentValue
}

// Valid returns true if the iterator is currently pointing to a valid key/value pair,
// and false if the iterator has reached the end of the block.
func (bi *BlockIterator) Valid() bool {
	return bi.valid
}

// SeekToFirst moves the iterator to the first key/value pair in the block.
func (bi *BlockIterator) SeekToFirst() bool {
	bi.offset = 0
	bi.valid = false

	bi.Next() // Move to the first entry

	return bi.valid
}
