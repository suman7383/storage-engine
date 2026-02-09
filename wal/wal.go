package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// -------------------------------------------------------------------------------
//
// Write path:
// write operation -> append to WAL -> fsync WAL (commit point) -> apply to MemTable
// -> return sequence number
//
// Flush / checkpoint path:
// MemTable -> (when full) -> flush to SSTable (fsync SSTable)
// -> update last_flushed_seq (checkpoint metadata)
//
// WAL fsync strategy:
// WAL entries are appended immediately.
// fsync may be done via group commit in a separate goroutine,
// triggered by:
//   1) elapsed time threshold (e.g. ~5ms), or
//   2) accumulated WAL size threshold
//
// During fsync, WAL writes are synchronized (e.g. via mutex) to
// ensure sequence numbers are only consumed after durability.
//
// -------------------------------------------------------------------------------

const MaxWALRecordSize = 1 * 1024 * 1024 // 1 MB

type WALRecord struct {
	Seq   uint64
	Op    OpType
	Key   []byte
	Value []byte // Nil for DELETE
}

type OpType uint8

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

var ErrRecordTooLarge = errors.New("record too large")

// Encodes the record to the below format:
//
// | record_len  | uint32 (Total bytes after this field)
// | seq_no      | uint64
// | op_type     | uint8  (1 = PUT, 2 = DELETE)
// | key_len     | uint32
// | value_len   | uint32   (0 for DELETE)
// | key bytes   |
// | value bytes |
// | checksum    | uint32 (CRC32 of everything above except record_len
func encodeRecord(seq uint64, op OpType, key, value []byte) (rec []byte, err error) {
	keyLen, valueLen := uint32(len(key)), uint32(len(value))

	// Calculate record_len (everything except record_len itself)
	recLen :=
		8 + // seq_no
			1 + // op_type
			4 + // key_len
			4 + // value_len
			int(keyLen) +
			int(valueLen) +
			4 // checksum

	if recLen > MaxWALRecordSize {
		return nil, ErrRecordTooLarge
	}

	rec = make([]byte, recLen+4) // Total size = record_len field + record_len bytes

	// record_len
	binary.LittleEndian.PutUint32(rec[0:4], uint32(recLen))
	offset := 4

	// Seq_no
	binary.LittleEndian.PutUint64(rec[offset:offset+8], seq)
	offset += 8

	// op_type
	rec[offset] = byte(op)
	offset += 1

	// key_len
	binary.LittleEndian.PutUint32(rec[offset:offset+4], keyLen)
	offset += 4

	// value_len
	binary.LittleEndian.PutUint32(rec[offset:offset+4], valueLen)
	offset += 4

	// key
	copy(rec[offset:offset+int(keyLen)], key)
	offset += int(keyLen)

	// value
	copy(rec[offset:offset+int(valueLen)], value)
	offset += int(valueLen)

	// Checksum
	checksum := crc32.ChecksumIEEE(rec[4:offset])
	binary.LittleEndian.PutUint32(rec[offset:offset+4], checksum)

	return rec, nil
}

var ErrPartialWrite = errors.New("partial write detected")
var ErrCorruptRecord = errors.New("corrupt record detected")
var ErrRecordMalformed = errors.New("malformed record detected")

// Decodes the encoded record to WALRecord
func decodeRecord(r io.Reader) (rec *WALRecord, bytesRead int, err error) {
	// read 4 bytes(record length field)
	var recLenBuf [4]byte
	n, err := io.ReadFull(r, recLenBuf[:])
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
	_, err = io.ReadFull(r, recBuf)
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

type WAL struct {
	fd      *os.File
	nextSeq uint64
	mu      sync.Mutex // Protects concurrent appends(for now we have single writer)
}

// Write creates a WALRecord with the data passed and calls the internal write
// function. If successfull, it returns the seq number, else it returns 0, error
// describing the error that occured
func (w *WAL) Put(key, value []byte) (seq uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if seq, err = w.write(OpPut, key, value); err != nil {
		return 0, err
	} else {
		w.nextSeq++

		return seq, nil
	}
}

func (w *WAL) Delete(key []byte) (seq uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if seq, err = w.write(OpDelete, key, nil); err != nil {
		return 0, err
	} else {
		w.nextSeq++

		return seq, nil
	}
}

// write is responsible for writing to the WAL.
func (w *WAL) write(op OpType, key, value []byte) (uint64, error) {
	seq := w.nextSeq

	rec, err := encodeRecord(seq, op, key, value)
	if err != nil {
		return 0, err
	}

	if _, err := w.fd.Write(rec); err != nil {
		return 0, err
	}

	// TODO:
	// Group Sync later
	if err := w.fd.Sync(); err != nil {
		return 0, err
	}

	return seq, nil
}
