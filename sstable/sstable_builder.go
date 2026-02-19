package sstable

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
)

// TODO: move these to somewhere else
const maxKeySize = 1 << 10                 // bytes
const SstMagic uint64 = 0x5353545630313031 // "SSTV0101"

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
	d.buff = d.buff[:0]

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
	bw         *bufio.Writer // maybe use default buffer size to 2 * blockSizeLimit
	filePath   string        // Temp path
	finalPath  string        // final .sst path
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
	// If the block is empty, don't proceed further
	if len(s.block.buff) == 0 {
		return 0, nil
	}

	// index entry for the current data block
	idx := indexEntries{
		lastKeyOfBlock: s.block.currBlockLastKey,
		blockOffset:    s.currOffset,
	}

	// write block buffer to file
	n, err = s.bw.Write(s.block.buff[:s.block.writeOffset])
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

	s.closed = true
}

// TODO:
// Called once after all entries added
//
// Flush last block(if not empty) -> write index block
// -> write footer -> rename filePath to finalPath -> mark finished
func (s *SstBuilder) finish() error {
	// Flush last block if not empty
	if s.block.writeOffset > 0 {
		if err := s.handleBlockSizeExceed(); err != nil {
			return err
		}
	}

	indexOffset := s.currOffset

	// write index block
	n, err := s.writeIndex()
	// TODO: handle error
	if err != nil {
		return err
	}

	// write footer block
	err = s.writeFooter(uint64(indexOffset), uint64(n))
	// TODO: Handle error
	if err != nil {
		return nil
	}

	// Cleanup(flush bw, sync fd, close fd, mark closed)

	// Rename the tempfile from filePath to finalPath

	s.finished = true

	return nil
}

// TODO:
// Sets the index for the current file.
// Called after writing all the blocks
//
// ---------- LAYOUT -----------------
// | 	key_len (uint32) 	   |
// | 	key bytes 		       |
// |	block_offset (uint64)  |
func (s *SstBuilder) writeIndex() (n int, err error) {
	n = 0
	baseLen := 4 + 8 // 4 bytes (key_len) + 8 bytes (block_offset)

	buf := make([]byte, baseLen+maxKeySize)

	for _, rec := range s.indexEntries {
		keyLen := len(rec.lastKeyOfBlock)
		offset := 0

		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(keyLen))
		offset += 4

		copy(buf[offset:offset+keyLen], rec.lastKeyOfBlock)
		offset += keyLen

		binary.LittleEndian.PutUint64(buf[offset:offset+8], uint64(rec.blockOffset))
		offset += 8

		nn, err := s.bw.Write(buf[:offset])
		if err != nil {
			return n + nn, err
		}

		n += nn // increment by bytes written
	}

	return n, nil
}

// TODO
// Sets the footer for the current file.
// Called after writing the index.
//
// Fixed size, located at end of file
//
//	---------- LAYOUT -----------------
//
// |	index_block_offset (uint64)		|
// |	index_block_size (uint64)		|
// |	magic_number (uint64)			| to detect file corruption, wrong file type, partial writes
//
// Total = 24 bytes
func (s *SstBuilder) writeFooter(indexOffset, indexBlockSize uint64) error {
	buf := make([]byte, 24) // index_block_offset + index_block_size + magic_number

	// index offset
	binary.LittleEndian.PutUint64(buf, indexOffset)
	offset := 8

	// index block size
	binary.LittleEndian.PutUint64(buf[offset:offset+8], indexBlockSize)
	offset += 8

	// magic number
	binary.LittleEndian.PutUint64(buf[offset:offset+8], SstMagic)
	offset += 8

	_, err := s.fd.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
