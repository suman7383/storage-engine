package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

type WALReader struct {
	br        *bufio.Reader
	bytesRead uint64
}

// WAL Reader
func NewReader(fd *os.File) *WALReader {
	return &WALReader{
		br:        bufio.NewReader(fd),
		bytesRead: 0,
	}
}

func (wr *WALReader) Next() (rec *WALRecord, eof bool, err error) {
	rec, n, err := wr.decodeRecord()
	if err != nil {
		if err == io.EOF {
			return nil, true, err
		} else {
			return nil, false, err
		}
	}

	wr.bytesRead += uint64(n)

	return rec, false, nil
}

// TODO
func (wr *WALReader) HasNext() (hasNext bool) {

	return false
}

// Decodes the encoded record to WALRecord
func (wr *WALReader) decodeRecord() (rec *WALRecord, bytesRead int, err error) {
	// read 4 bytes(record length field)
	var recLenBuf [4]byte
	n, err := io.ReadFull(wr.br, recLenBuf[:])
	if err != nil {
		// Clean end of WAL
		if err == io.EOF && n == 0 {
			return nil, 0, io.EOF
		}

		// Parital length field(corruption)
		return rec, 0, ErrPartialWrite
	}

	recLen := binary.LittleEndian.Uint32(recLenBuf[:])

	// Minimum possible record:
	// seq(8) + op(1) + keyLen(4) + valueLen(4) + checksum(4)
	const minRecordLen = 8 + 1 + 4 + 4 + 4
	if recLen < minRecordLen {
		return nil, 0, ErrRecordMalformed
	}

	if recLen > MaxWALRecordSize {
		return nil, 0, ErrRecordMalformed
	}

	// read the rest(record_len size byte)
	recBuf := make([]byte, recLen)
	_, err = io.ReadFull(wr.br, recBuf)
	if err != nil {
		return rec, 0, ErrPartialWrite
	}

	// Record is fully read
	bytesRead = int(recLen) + 4 // record_len bytes + record_len field size(4 byte)

	// verify checksum
	dataEnd := recLen - 4
	storedChecksum := binary.LittleEndian.Uint32(recBuf[dataEnd:])
	calculatedChecksum := crc32.ChecksumIEEE(recBuf[:dataEnd])

	if storedChecksum != calculatedChecksum {
		return rec, 0, ErrCorruptRecord
	}

	// Decode fields
	offset := 0

	// seq_no
	seq := binary.LittleEndian.Uint64(recBuf[offset : offset+8])
	offset += 8

	// op_type
	op := OpType(recBuf[offset])
	offset += 1

	// key_len
	keyLen := binary.LittleEndian.Uint32(recBuf[offset : offset+4])
	offset += 4

	// value_len
	valueLen := binary.LittleEndian.Uint32(recBuf[offset : offset+4])
	offset += 4

	if op == OpDelete && valueLen != 0 {
		return nil, 0, ErrRecordMalformed
	}

	// Check if record is malformed
	if offset+int(keyLen)+int(valueLen)+4 != int(recLen) {
		return nil, 0, ErrRecordMalformed
	}

	key := make([]byte, keyLen)
	copy(key, recBuf[offset:offset+int(keyLen)])
	offset += int(keyLen)

	val := make([]byte, valueLen)
	copy(val, recBuf[offset:offset+int(valueLen)])
	offset += int(valueLen)

	rec = &WALRecord{
		Seq:   seq,
		Op:    op,
		Key:   key,
		Value: val,
	}

	return rec, bytesRead, nil
}
