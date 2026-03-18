package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"

	"github.com/suman7383/storage-engine/internalkey"
)

type SstReader struct {
	fd       *os.File
	fileSize int64

	indexBuf     []byte
	indexEntries []readerIndexEntries
	indexOffset  uint64
	indexSize    uint64

	smallestKey internalkey.InternalKey
	largestKey  internalkey.InternalKey
}

// Creates and initializes(parses footer, index) the sst reader.
func NewSstReader(fd *os.File, fileSize int64, smallestKey, largestKey []byte) (*SstReader, error) {

	s := &SstReader{
		fd:       fd,
		fileSize: fileSize,

		smallestKey: smallestKey,
		largestKey:  largestKey,
	}

	indexEntryCount, indexOffset, indexSize, err := s.readFooter()
	if err != nil {
		return nil, err
	}

	err = s.loadIndex(indexEntryCount, indexOffset, indexSize)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *SstReader) Get(key internalkey.InternalKey) (value []byte, ok bool) {
	// Compare if the key lies between smallestKey and largestKey
	// If not return early, as it is not present in this SST
	if key.IsLessThan(s.smallestKey) || key.IsGreaterThan(s.largestKey) {
		return nil, false
	}

	// Binary search in indexEntries to find in which block key exists
	blockOffset, found := s.binarySearchIndex(key)
	if !found {
		return nil, false
	}

	// Read that data block and parse the entries
	block := s.readBlock(blockOffset)

	// Linear search inside the block
	return s.linearSearchBlock(block, key)
}

// ----  Binary Layout ----
//
// | KeyLen  			| 4 bytes
// | ValueLen 			| 4 bytes
// | InternalKey      	|
// | Value    			|
func (s *SstReader) linearSearchBlock(block []byte, key internalkey.InternalKey) (value []byte, ok bool) {
	offset := 0

	for offset < len(block) {
		keyLen := binary.LittleEndian.Uint32(block[offset : offset+4])
		offset += 4

		valueLen := binary.LittleEndian.Uint32(block[offset : offset+4])
		offset += 4

		ik := internalkey.InternalKey(block[offset : offset+int(keyLen)])
		offset += int(keyLen)

		if ik.EqualUserKeys(key) && ik.IsPut() {
			value = make([]byte, valueLen)
			copy(value, block[offset:offset+int(valueLen)])

			return value, true
		}

		offset += int(valueLen)
	}

	return nil, false
}

func (s *SstReader) readBlock(blockOffset uint64) []byte {
	// Seek to the block offset
	s.fd.Seek(int64(blockOffset), io.SeekStart)
	br := bufio.NewReader(s.fd)

	// Read block-size
	bs := make([]byte, 4)
	_, err := io.ReadFull(br, bs)
	if err != nil {
		slog.Error(err.Error())
		panic("could not read block-size")
	}

	blockSize := binary.LittleEndian.Uint32(bs)

	buf := make([]byte, blockSize)

	// Read block-size into buffer
	_, err = io.ReadFull(br, buf)
	if err != nil {
		panic("block is corrupt")
	}

	return buf
}

// binarySearchIndex performs binary search on the indexEntries and returns the
// block offset.
// ok indicates whether the block is found or not
func (s *SstReader) binarySearchIndex(key internalkey.InternalKey) (uint64, bool) {
	l, h := 0, len(s.indexEntries)

	for l < h {
		m := l + (h-l)/2

		ie := s.indexEntries[m]

		// compare the key at mid entry
		mk := internalkey.InternalKey(s.indexBuf[ie.keyStart : ie.keyStart+ie.keyLen])

		// Key found
		if mk.EqualUserKeys(key) {
			return ie.blockOffset, true
		}

		if key.Compare(mk) < 0 {
			// Key is left of mid
			h = m - 1
		} else {
			// Key is right of mid
			l = m + 1
		}
	}

	return 0, false
}

const footerSize = 28 //bytes

func (s *SstReader) readFooter() (indexEntryCount uint32, indexOffset, indexSize uint64, err error) {
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
		offset += int(keyLen)

		ie.blockOffset = binary.LittleEndian.Uint64(indexBuf[offset : offset+8])
		offset += 8

		s.indexEntries = append(s.indexEntries, ie)
	}

	return nil
}
