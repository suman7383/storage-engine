package sstable

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"
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
func NewSstReader(fd *os.File, fileSize int64, smallestKey, largestKey internalkey.InternalKey) (*SstReader, error) {
	log.Printf("[SST READER] smallestKey: %v, largestKey: %v",
		string(smallestKey.UserKey()),
		string(largestKey.UserKey()))

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

	// DEBUGGING INDEX ENTRIES
	// for _, e := range s.indexEntries {
	// 	k := internalkey.InternalKey(s.indexBuf[e.keyStart : e.keyStart+e.keyLen])
	// 	log.Printf("[SST READER] index entry info. largestKey: %v, blockOffset: %v", string(k.UserKey()), e.blockOffset)
	// }

	return s, nil
}

// GetBlock returns the block containing the key.
// If the key is not present in this SST file, it returns nil.
//
// This function does not perform a linear search inside the block.
// It only finds the block that may or may not contain the key.
func (s *SstReader) GetBlock(key internalkey.InternalKey) *Block {
	// Compare if the key lies between smallestKey and largestKey
	// If not return early, as it is not present in this SST
	if (key.IsLessThan(s.smallestKey) && key.CompareUserKeys(s.smallestKey) != 0) || key.IsGreaterThan(s.largestKey) {
		return nil
	}

	log.Printf("[SST READER] key lies in current SST")

	// Binary search in indexEntries to find in which block key exists
	blockOffset, found := s.binarySearchIndex(key)
	if !found {
		log.Println("[SST READER] not found in index")
		return nil
	}

	// Read that data block and parse the entries
	return s.readBlock(blockOffset)
}

// Get returns the value corresponding to the key, if it exists in this SST file. Otherwise returns nil.
// If the key is not present in this SST file, it returns nil, false
// If the key is present, but it is a tombstone, it returns nil, true
// If the key is present, and it is a put, it returns the value, true
func (s *SstReader) Get(key internalkey.InternalKey) (value []byte, ok bool) {
	block := s.GetBlock(key)
	if block == nil {
		return nil, false
	}

	// Linear search inside the block
	value, isPresent, isTombstone := block.linearSearch(key)

	if !isPresent {
		return nil, false
	}

	if isTombstone {
		return nil, true
	}

	return value, true
}

// readBlock reads the block placed after the blockOffset and returns it.
func (s *SstReader) readBlock(blockOffset uint64) *Block {
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

	return &Block{data: buf}
}

// ReadBlockAtIndex reads the block at the given index and returns it.
// It returns an error if the index is out of bounds.
func (s *SstReader) ReadBlockAtIndex(index int) (*Block, error) {
	if index < 0 || index >= len(s.indexEntries) {
		return nil, errors.New("index out of bounds")
	}

	blockIdx := s.indexEntries[index]

	return s.readBlock(blockIdx.blockOffset), nil
}

// NumberOfBlocks returns the number of blocks in the SST file.
func (s *SstReader) NumberOfBlocks() int {
	return len(s.indexEntries)
}

// NewIterator returns an iterator over the blocks of the SST file.
// Use SeekToFirst to initialize iterator at the first block.
// Then use Next to iterate over the blocks.
func (s *SstReader) NewIterator() *SstIterator {
	return &SstIterator{
		reader:            s,
		blockIterator:     nil,
		currentBlockIndex: -1,
	}
}

// binarySearchIndex performs binary search on the indexEntries and returns the
// block offset.
// ok indicates whether the block is found or not
func (s *SstReader) binarySearchIndex(key internalkey.InternalKey) (uint64, bool) {
	l, h := 0, len(s.indexEntries)-1
	result := -1

	for l <= h {
		m := l + (h-l)/2

		ie := s.indexEntries[m]
		mk := internalkey.InternalKey(s.indexBuf[ie.keyStart : ie.keyStart+ie.keyLen])

		// KEY FIX: use mk.Compare(key), not key.Compare(mk)
		if mk.Compare(key) >= 0 {
			result = m
			h = m - 1 // go LEFT to find first valid block
		} else {
			l = m + 1
		}
	}

	if result == -1 {
		return 0, false
	}

	mk := internalkey.InternalKey(s.indexBuf[s.indexEntries[result].keyStart : s.indexEntries[result].keyStart+s.indexEntries[result].keyLen])

	log.Printf("[INDEX RESULT] picked block with largestKey: %s", string(mk.UserKey()))

	return s.indexEntries[result].blockOffset, true
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

	log.Printf("[SST READER] indexEntry: %v, indexOffset: %v, indexSize: %v, magicNum: %v",
		indexEntryCount, indexOffset, indexSize, magicNum)

	// Validate magic number
	if magicNum != SstMagic {
		log.Printf("[SST] magicNum found: %v, sstMagic: %v", magicNum, SstMagic)
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
