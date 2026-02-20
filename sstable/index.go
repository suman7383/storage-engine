package sstable

type indexEntries struct {
	lastKeyOfBlock []byte
	blockOffset    int64
}
