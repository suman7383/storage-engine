package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
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

const MaxWALRecordSize = 1 * 1024 * 1024   // 1 MB
const MaxWALSegmentSize = 64 * 1024 * 1024 // 64 MB

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
func encodeRecord(seq uint64, op OpType, key, value []byte) (rec_size uint64, rec []byte, err error) {
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
		return 0, nil, ErrRecordTooLarge
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

	return uint64(recLen + 4), rec, nil
}

var ErrPartialWrite = errors.New("partial write detected")
var ErrCorruptRecord = errors.New("corrupt record detected")
var ErrRecordMalformed = errors.New("malformed record detected")

type WALSegmentMeta struct {
	StartSeq uint64
	EndSeq   uint64
	State    WALState
}

type WALState string

const (
	WALSealed WALState = "sealed"
	WALActive WALState = "active"
)

type WAL struct {
	fd                *os.File
	nextSeq           uint64
	mu                sync.Mutex     // Protects concurrent appends(for now we have single writer)
	active            WALSegmentMeta // Meta data about current active wal segments
	ActiveSegmentSize uint64         // Size of the current WAL
}

// Put creates a WALRecord with the data passed and calls the internal write
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

	n, rec, err := encodeRecord(seq, op, key, value)
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

	// Update the activeSegmentSize and endSeq
	w.updateSizeAndEndSeq(n, seq)

	return seq, nil
}

// Updates the activeSegmentSize and the EndSeq of the active WAL segment
func (w *WAL) updateSizeAndEndSeq(delta uint64, endSeq uint64) {
	w.addActiveSegmentSize(delta)
	w.updateEndSeq(endSeq)
}

func (w *WAL) addActiveSegmentSize(delta uint64) {
	w.ActiveSegmentSize += delta
}

func (w *WAL) updateEndSeq(end uint64) {
	w.active.EndSeq = end
}
