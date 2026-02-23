package sstable

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
)

type SstReader struct {
	fd       *os.File
	fileSize int64

	indexBuf     []byte
	indexEntries []readerIndexEntries
	indexOffset  uint64
	indexSize    uint64

	smallestKey []byte
	largestKey  []byte
}

func (s *SstReader) Get(key []byte) (value []byte) {
	// Compare if the key lies between smallestKey and largestKey
	// If not return early, as it is not present in this SST

	// Binary search in indexEntries to find in which block key exists

	// Read that data block and parse the entries

	// Linear search inside the block

	return nil
}

const footerSize = 28 //bytes

func (s *SstReader) readFooter() (indexEntryCount uint32, indexOffset, indeSize uint64, err error) {
	// Seek to footer start from end
	if _, err := s.fd.Seek(-footerSize, io.SeekEnd); err != nil {
		return 0, 0, 0, err
	}

	buf := make([]byte, footerSize)
	if _, err := io.ReadFull(s.fd, buf); err != nil {
		return 0, 0, 0, err
	}

	return s.parseFooter(buf)
}

var ErrFooterCorrupt = errors.New("footer is corrupt")

//	---------- LAYOUT -----------------
//
// |    indexCount (uint32)	   | header of index
// |	index_block_offset (uint64)		| 8 bytes
// |	index_block_size (uint64)		| 8 bytes
// |	magic_number (uint64)			| 8 bytes (to detect file corruption, wrong file type, partial writes)
func (s *SstReader) parseFooter(buf []byte) (indexEntryCount uint32, indexOffset, indexSize uint64, err error) {
	offset := 0

	indexEntryCount = binary.LittleEndian.Uint32(buf[offset : offset+4])
	offset += 4

	indexOffset = binary.LittleEndian.Uint64(buf[offset : offset+8])
	offset += 8

	indexSize = binary.LittleEndian.Uint64(buf[offset : offset+8])
	offset += 8

	magicNum := binary.LittleEndian.Uint64(buf[offset : offset+8])
	offset += 8

	// Validate magic number
	if magicNum != SstMagic {
		return 0, 0, 0, ErrFooterCorrupt
	}

	return indexEntryCount, indexOffset, indexSize, nil
}

var ErrIndexCorrupt = errors.New("Index is corrupted")

// ---------- Index LAYOUT -----------------
// | 	key_len (uint32) 	   |
// | 	key bytes 		       |
// |	block_offset (uint64)  |
func (s *SstReader) loadIndex(indexEntryCount uint32, indexOffset, indexSize uint64) error {
	if _, err := s.fd.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return err
	}

	indexBuf := make([]byte, indexSize)
	_, err := io.ReadFull(s.fd, indexBuf)
	if err != nil {
		return err
	}

	s.indexBuf = indexBuf
	s.indexEntries = make([]readerIndexEntries, 0, indexEntryCount)

	offset := 0

	for offset < int(indexSize) {
		ie := readerIndexEntries{}

		keyLen := binary.LittleEndian.Uint32(indexBuf[offset : offset+4])
		offset += 4

		// If crossing the indexSize
		if uint64(offset)+uint64(keyLen)+8 > indexSize {
			return ErrIndexCorrupt
		}

		ie.keyStart = uint32(offset)
		ie.keyLen = keyLen

		ie.blockOffset = binary.LittleEndian.Uint64(indexBuf[offset : offset+8])
		offset += 8

		s.indexEntries = append(s.indexEntries, ie)
	}

	return nil
}
