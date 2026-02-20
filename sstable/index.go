package sstable

type indexEntries struct {
	lastKeyOfBlock []byte
	blockOffset    int64
}

type readerIndexEntries struct {
	blockOffset uint64
	keyStart    uint32
	keyLen      uint32
}
