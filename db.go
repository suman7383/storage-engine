package storageengine

import (
	"sync"

	"github.com/suman7383/storage-engine/memtable"
	"github.com/suman7383/storage-engine/sstable"
	"github.com/suman7383/storage-engine/wal"
)

// TODO
type Options struct {
}

// TODO
type DB struct {
	// WAL
	walDir      string // path to WAL directory
	wal         *wal.WAL
	walSegments []wal.WALSegmentMeta // Meta-data about all wal segments

	// Memtable
	activeMem  *memtable.Memtable
	frozenMems []*memtable.Memtable

	// SST
	levels [][]*sstable.SstReader

	nextSeq uint64

	options *Options
	mu      sync.RWMutex
}

// TODO:
// Load manifests
// Discover SST files
// Recover wal
// replay WAL into a new memtable
// restore nextSeq
func (d *DB) Open() {
	// TODO: Load Manifests

	// TODO: Discover SST files

	// TODO: Initialize New memtable

	// TODO: Recover WAL

}

// TODO:
// Allocate seq number
// append to wal
// insert into active memtable
// if memtable full -> freeze + schedule flush
func (d *DB) Put() {

}

// TODO:
// search order:
// 1> active memtable
// 2> Frozen memtables (newest -> oldest)
// 3> L0 SSTables (newest -> oldest)
// 4> L1+
//
// Return first visible version <= snapshot seq
func (d *DB) Get() {

}
