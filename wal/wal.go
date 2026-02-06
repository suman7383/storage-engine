package wal

import (
	"os"
	"sync"
)

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

func encodeRecord(seq uint64, op OpType, key, value []byte) *WALRecord {
	return &WALRecord{
		Seq:   seq,
		Op:    op,
		Key:   key,
		Value: value,
	}
}

type WAL struct {
	fd      *os.File
	nextSeq uint64
	mu      sync.Mutex // Protects concurrent appends(for now we have single writer)
}

// TODO:
// Write creates a WALRecord with the data passed and calls the internal write
// function. If successfull, it returns the seq number, else it returns 0, error
// describing the error that occured
func (w *WAL) Write(key, value []byte) (uint64, error) {
	return 0, nil
}

// TODO:
// write is responsible for writing to the WAL.
func (w *WAL) write(op OpType, key, value []byte) (uint64, error) {

	return 0, nil
}
