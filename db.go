package storageengine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/suman7383/storage-engine/memtable"
	"github.com/suman7383/storage-engine/op"
	"github.com/suman7383/storage-engine/sstable"
	"github.com/suman7383/storage-engine/wal"
)

// TODO
type Options struct {
	WalDir string
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

	mu sync.RWMutex

	isInitialized bool
}

func NewDB(options Options) *DB {
	return &DB{
		walDir:      options.WalDir,
		walSegments: make([]wal.WALSegmentMeta, 0, 10),

		frozenMems: make([]*memtable.Memtable, 0, 10),

		levels: make([][]*sstable.SstReader, 0, 5),

		isInitialized: false,
	}
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

	log.Printf("[WAL] Replay complete. DB struct: %+v", db)

	// Open a new wal
	var activeWalID uint64 = 0

	if len(db.walSegments) > 0 {
		activeWalID = db.walSegments[len(db.walSegments)-1].Id + 1
	}

	activeWalPath := filepath.Join(db.walDir, fmt.Sprintf("wal-%06d.log", activeWalID))
	wfd, err := os.OpenFile(
		activeWalPath,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		log.Fatalf("could not create new wal file: wal-%06d.log, err: %v", activeWalID, err)
	}

	db.wal = wal.NewWAL(wfd, activeWalID, activeWalPath)

	db.isInitialized = true
}

func (db *DB) IsInitialized() bool {
	return db.isInitialized
}

// TODO:
// Allocate seq number
// append to wal
// insert into active memtable
// if memtable full -> freeze + schedule flush
func (db *DB) Put(userKey, value []byte) (ok bool, err error) {
	if !db.isInitialized {
		log.Fatal("db not initialized. Call db.Open()")
	}

	return db.apply(db.nextSeq, userKey, value, op.OpPut)
}

// TODO:
// search order:
// 1> active memtable
// 2> Frozen memtables (newest -> oldest)
// 3> L0 SSTables (newest -> oldest)
// 4> L1+
//
// Return first visible version <= snapshot seq
func (db *DB) Get(userKey []byte) (value []byte, ok bool) {
	if !db.isInitialized {
		log.Fatal("db not initialized. Call db.Open()")
	}

	var rec *memtable.Node
	rec, ok = db.activeMem.Get(userKey, db.nextSeq-1)
	if !ok {
		// Search in frozen memtables
		rec, ok = db.searchFrozenMemtables(userKey, db.nextSeq-1)

		if !ok {
			// TODO: Search SST
			return nil, false
		}
	}

	value = make([]byte, len(rec.Value))
	copy(value, rec.Value)

	return value, true
}

func (db *DB) Delete(userKey []byte) (ok bool, err error) {
	if !db.isInitialized {
		log.Fatal("db not initialized. Call db.Open()")
	}

	return db.apply(db.nextSeq, userKey, nil, op.OpDelete)
}

func (db *DB) apply(seq uint64, userKey, value []byte, operation op.OpType) (ok bool, err error) {
	if !db.isInitialized {
		log.Fatal("db not initialized. Call db.Open()")
	}

	switch operation {
	case op.OpPut:
		seq, err = db.wal.Put(userKey, value, db.nextSeq)
	case op.OpDelete:
		seq, err = db.wal.Delete(userKey, db.nextSeq)
	default:
		return false, fmt.Errorf("invalid operation, %v", operation)
	}

	if err != nil {
		return false, err
	}

	// Increment the nextSeq
	db.nextSeq++

	// Insert to active memtable
	_, err = db.activeMem.Apply(userKey, value, seq, operation)

	// TODO: Checking for active memtable size

	return true, err
}

// Searches the frozen memtables from newest to oldest
func (db *DB) searchFrozenMemtables(userKey []byte, snapshotSeq uint64) (rec *memtable.Node, ok bool) {
	log.Print("[GET] searching in frozen memtable")
	for i := len(db.frozenMems) - 1; i >= 0; i-- {
		memtable := db.frozenMems[i]

		rec, ok := memtable.Get(userKey, snapshotSeq)
		if ok {
			return rec, ok
		}
	}

	return nil, false
}
