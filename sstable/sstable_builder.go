package sstable

import (
	"log"
	"os"
)

type indexEntries struct {
	lastKeyOfBlock []byte
	blockOffset    int64
}

type dataBlock struct {
	buff              []byte
	writeOffset       int
	sizeLimit         int
	currBlockFirstKey []byte
	currBlockLastKey  []byte
}

func (d *dataBlock) resetBlock() {
	// logically reset the buff
	d.writeOffset = 0

	// reset the currBlockFirstKey, currBlockLastKey
	// length becomes 0, capacity remains same.
	// This is okay since we put a limit to the maxKeySize to ensure this buffer
	// does not explode in size due to capacity increase
	d.currBlockFirstKey = d.currBlockFirstKey[:0]
	d.currBlockLastKey = d.currBlockLastKey[:0]
}

// TODO:
// IMPORTANT: Allocate the smallestKey, largestKey, dataBlock.currBlockFirstKey
// and dataBlock.currBlockLastKey fields with maxKeySize capacity.
type SstBuilder struct {
	fd         *os.File
	filePath   string // Temp path
	finalPath  string // final .sst path
	currOffset int64

	// Block
	block *dataBlock

	// Index
	indexEntries []indexEntries

	// Meta-data
	smallestKey []byte
	largestKey  []byte
	entryCount  int

	// Flags
	finished bool
	closed   bool
}

// TODO:
// Encode entry -> Append to block buffer -> update lastKey
// -> (if first entry in block) -> set firstKey
// -> (if block size exceeded) -> flush block
func (s *SstBuilder) Add(key, value []byte, seq uint64, kind uint8) error {
	b := EncodeEntry(key, value, seq, kind)

	// If adding the current bytes results in overflow of the current block,
	// then flush the current block and reset the block
	if s.block.writeOffset+len(b) > s.block.sizeLimit {
		if err := s.handleBlockSizeExceed(); err != nil {
			return err
		}
	}

	// Copy into the window of the buffer
	// from block.writeOffset to block.writeOffset + len(b)
	copy(s.block.buff[s.block.writeOffset:s.block.writeOffset+len(b)], b)
	s.block.writeOffset += len(b)

	// Update the first key of current block if this is the first key in the block
	if len(s.block.currBlockFirstKey) == 0 {
		s.block.currBlockFirstKey = append(s.block.currBlockFirstKey, key...)
	}

	// Update the last key of current block
	s.block.currBlockLastKey = s.block.currBlockLastKey[:0]
	s.block.currBlockLastKey = append(s.block.currBlockLastKey, key...)

	// If very first entry into the sst, mark as smallest key.
	// Assuming the memtable flush follows strict ordering
	if s.entryCount == 0 {
		s.smallestKey = append(s.smallestKey, key...)
	}

	// Update the largestKey
	s.largestKey = append(s.largestKey, key...)

	// update meta-data
	s.entryCount++

	return nil
}

// TODO
func (s *SstBuilder) handleBlockSizeExceed() error {
	// flush the current block
	n, err := s.flushBlock()
	if err != nil {
		// close the file and delete it, also either exit process(panic) or
		// mark db as read-only as db consistency cannot be guaranteed now.
		s.markFailed(err)
		return err
	}

	s.currOffset += int64(n)

	return nil
}

// markFailed indicates something went wrong during either writing or flushing
// and so the db consistency cannot be guaranteed.
//
// It deletes the temporary sst file created and does cleanup before exiting
func (s *SstBuilder) markFailed(err error) {
	// Closes the fd and deletes the temp file
	s.cleanup()

	log.Fatalf("[SYSTEM] exiting due to sst failure. err: %s", err.Error())
}

// TODO:
// Write block buffer to file -> record blockOffset
// -> append index entry -> clear block buffer
// -> reset block first key/last key
// returns the bytes written to file
func (s *SstBuilder) flushBlock() (n int, err error) {
	// index entry for the current data block
	idx := indexEntries{
		lastKeyOfBlock: s.block.currBlockLastKey,
		blockOffset:    s.currOffset,
	}

	// write block buffer to file
	n, err = s.fd.Write(s.block.buff[:s.block.writeOffset])
	if err != nil {
		return 0, nil
	}

	// Append the current block index entry
	s.indexEntries = append(s.indexEntries, idx)

	// Reset the block
	s.block.resetBlock()

	return n, nil
}

func (s *SstBuilder) cleanup() {
	s.fd.Close()

	// delete the current temporary file
	os.Remove(s.filePath)
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
func (s *SstBuilder) writeIndex() {

}

// TODO
// Sets the footer for the current file.
// Called after writing the index.
func (s *SstBuilder) writeFooter() {

}
