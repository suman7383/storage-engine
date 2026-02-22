package sstable

import (
	"bufio"
	"encoding/binary"
	"os"
)

// TODO: move these to somewhere else
const maxKeySize = 1 << 10                 // bytes
const SstMagic uint64 = 0x5353545630313031 // "SSTV0101"

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

func NewSstBuilder(fd *os.File, blockSizeLimit int) *SstBuilder {
	block := &dataBlock{
		buff:              make([]byte, 0, blockSizeLimit),
		writeOffset:       0,
		sizeLimit:         blockSizeLimit,
		currBlockFirstKey: make([]byte, 0, maxKeySize),
		currBlockLastKey:  make([]byte, 0, maxKeySize),
	}

	return &SstBuilder{
		fd: fd,
		bw: bufio.NewWriter(fd),

		currOffset: 0,

		block: block,

		indexEntries: make([]indexEntries, 0, 30), // TODO: Change the capacity from hard-coded value later

		smallestKey: make([]byte, 0, maxKeySize),
		largestKey:  make([]byte, 0, maxKeySize),

		entryCount: 0,
		finished:   false,
		closed:     false,
	}
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

// var ErrBlockWrite = errors.New("error writing sst block")

func (s *SstBuilder) handleBlockSizeExceed() error {
	// flush the current block
	n, err := s.flushBlock()
	if err != nil {
		return err
	}

	s.currOffset += int64(n)

	return nil
}

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

	// write block buffer to writer
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

// Called once after all entries added
//
// Flush last block(if not empty) -> write index block
// -> write footer -> rename filePath to finalPath -> mark finished
func (s *SstBuilder) Finish() error {
	// Flush last block if not empty
	if s.block.writeOffset > 0 {
		if err := s.handleBlockSizeExceed(); err != nil {
			return err
		}
	}

	indexOffset := s.currOffset

	// write index block
	n, err := s.writeIndex()
	if err != nil {
		return err
	}

	s.currOffset += int64(n)

	// write footer block
	err = s.writeFooter(uint64(indexOffset), uint64(n))
	if err != nil {
		return err
	}

	// flush bw
	err = s.bw.Flush()
	if err != nil {
		return err
	}

	s.finished = true

	return nil
}

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

// Sets the footer for the current file.
// Called after writing the index.
//
// Fixed size, located at end of file
//
//	---------- LAYOUT -----------------
//
// |	indexEntryCount (uint32)    	| header of index block
// |	index_block_offset (uint64)		|
// |	index_block_size (uint64)		|
// |	magic_number (uint64)			| to detect file corruption, wrong file type, partial writes
//
// Total = 28 bytes
func (s *SstBuilder) writeFooter(indexOffset, indexBlockSize uint64) error {
	buf := make([]byte, 28) // index_block_offset + index_block_size + magic_number
	offset := 0

	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(s.indexEntries)))
	offset += 4

	// index offset
	binary.LittleEndian.PutUint64(buf[offset:offset+8], indexOffset)
	offset += 8

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
