package sstable

import "os"

type IndexEntries struct {
	LastKeyOfBlock []byte
	BlockOffset    int64
}

type SstBuilder struct {
	fd         *os.File
	currOffset int64

	// Block
	block struct {
		buff              []byte
		sizeLimit         int
		currBlockFirstKey []byte
		currBlockLastKey  []byte
	}

	// Index
	indexEntries []IndexEntries

	// Meta-data
	smallesKey []byte
	largestKey []byte
	entryCount int

	// Flags
	finished bool
	closed   bool
}

// TODO:
// Encode entry -> Append to block buffer -> update lastKey
// -> (if first entry in block) -> set firstKey
// -> (if block size exceeded) -> flush block
func (s *SstBuilder) Add(key, value []byte, seq uint64, kint uint8) error {

}

// TODO:
// Write block buffer to file -> record blockOffset
// -> append index entry -> clear block buffer
// -> reset block first key/last key
func (s *SstBuilder) flushBlock() error {

}

// TODO:
// Called once after all entries added
//
// Flush last block(if not empty) -> write index block
// -> write footer -> mark finished
func (s *SstBuilder) finish() {

}

// TODO:
// Sets the index for the current file.
// Called after writing all the blocks
func (s *SstBuilder) index() {

}

// TODO
// Sets the footer for the current file.
// Called after writing the index.
func (s *SstBuilder) footer() {

}

// TODO
// Close the file descriptor
func (s *SstBuilder) close() {

}
