package sstable

import "os"

type IndexEntries struct {
	LastKeyOfBlock []byte
	BlockOffset    int64
}

type SstBuilder struct {
	fd         *os.File
	currOffset int64

	// Block
	block struct {
		buff              []byte
		sizeLimit         int
		currBlockFirstKey []byte
		currBlockLastKey  []byte
	}

	// Index
	indexEntries []IndexEntries

	// Meta-data
	smallesKey []byte
	largstKey  []byte
	entryCount int

	// Flags
	finished bool
	closed   bool
}
