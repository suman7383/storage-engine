package sstable

import "os"

type SstReader struct {
	fd       *os.File
	fileSize int64

	indexEntries []indexEntries
	indexOffset  uint64
	indexSize    uint64

	smallestKey []byte
	largestKey  []byte
}
