package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"sync"
)

//
// -------------------------------------------------------------------------------
//
// write operation -> wal -> (if successful) -> memtable -> finally returns seq number
// memtable -> (if full) -> sstable -> updated latest seq number(for checkpointing)
//
// In a separate goroutine, frequently check for conditions and fsync wal
// condition : 1> more than ~5 ms
// 			   2> or some size reached
// block the wal from writing(mutex) when doing this
//
// -------------------------------------------------------------------------------
//

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
func encodeRecord(seq uint64, op OpType, key, value []byte) (rec []byte) {
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

	return rec
}

// TODO:
// Decodes the encoded record to WALRecord
func decodeRecord(rec []byte) *WALRecord {

}

type WAL struct {
	fd      *os.File
	nextSeq uint64
	mu      sync.Mutex // Protects concurrent appends(for now we have single writer)
}

// Write creates a WALRecord with the data passed and calls the internal write
// function. If successfull, it returns the seq number, else it returns 0, error
// describing the error that occured
func (w *WAL) Write(key, value []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if seq, err := w.write(OpPut, key, value); err != nil {
		return 0, err
	} else {
		w.nextSeq++

		return seq, nil
	}
}

// TODO
func (w *WAL) Delete(key []byte) {

}

// write is responsible for writing to the WAL.
func (w *WAL) write(op OpType, key, value []byte) (uint64, error) {
	seq := w.nextSeq

	rec := encodeRecord(seq, op, key, value)

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
