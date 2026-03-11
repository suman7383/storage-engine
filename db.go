package storageengine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
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
func (db *DB) Open() {
	// TODO: Load Manifests

	// TODO: Discover SST files

	// TODO: Initialize New memtable
	db.activeMem = memtable.NewMemtable(memtable.NewSkipList())

	// Recover WAL
	err := db.replayWAL()
	if err != nil {
		log.Fatal(err)
	}

	// Open a new wal
	var activeWalID uint64 = 0

	if len(db.walSegments) > 0 {
		activeWalID = db.walSegments[len(db.walSegments)-1].Id
	}

	activeWalPath := filepath.Join(db.walDir, fmt.Sprintf("wal-%06d.log", activeWalID))
	wfd, err := os.Create(activeWalPath)
	if err != nil {
		log.Fatalf("could not create new wal file: wal-%06d.log, err: %v", activeWalID, err)
	}

	db.wal = wal.NewWAL(wfd, activeWalID, activeWalPath)
}

// TODO:
// Allocate seq number
// append to wal
// insert into active memtable
// if memtable full -> freeze + schedule flush
func (db *DB) Put() {

}

// TODO:
// search order:
// 1> active memtable
// 2> Frozen memtables (newest -> oldest)
// 3> L0 SSTables (newest -> oldest)
// 4> L1+
//
// Return first visible version <= snapshot seq
func (db *DB) Get() {

}
