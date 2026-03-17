package storageengine

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/suman7383/storage-engine/memtable"
	"github.com/suman7383/storage-engine/op"
	"github.com/suman7383/storage-engine/sstable"
	"github.com/suman7383/storage-engine/wal"
)

// TODO
type Options struct {
	StorageDir            string
	memtableMaxSize       uint64
	maxImmutableMemtables int
}

// TODO
type DB struct {
	// Storage
	storageDir string

	// Manifest
	manifest *manifest

	// WAL
	wal         *wal.WAL
	walSegments []wal.WALSegmentMeta // Meta-data about all wal segments

	// Memtable
	activeMem             *memtable.Memtable
	frozenMems            []*memtable.Memtable
	flushChan             chan *memtable.Memtable
	memtableMaxSize       uint64
	maxImmutableMemtables int

	// SST
	levels    [][]*sstable.SstReader
	nextSstID int

	nextSeq uint64

	mu sync.RWMutex

	isInitialized bool

	errState bool  // Indicates whether db is in error state(closed for writes)
	err      error // If errState is true, this represents the error
}

const memtableMaxSize = 16 * 1024 * 1024 // 16MB
const blockSize = 4 * 1024               // 4KB

func NewDB(options Options) *DB {
	return &DB{
		storageDir:  options.StorageDir,
		walSegments: make([]wal.WALSegmentMeta, 0, 10),

		frozenMems:            make([]*memtable.Memtable, 0, 10),
		flushChan:             make(chan *memtable.Memtable, options.maxImmutableMemtables),
		memtableMaxSize:       options.memtableMaxSize,
		maxImmutableMemtables: options.maxImmutableMemtables, // At most 2 immutable memtables waiting for flush

		levels: make([][]*sstable.SstReader, 0, 5),

		isInitialized: false,

		errState: false,
	}
}

// TODO:
// Load manifests
// Discover SST files
// Recover wal
// replay WAL into a new memtable
// restore nextSeq
func (db *DB) Open() {
	err := os.MkdirAll(db.storageDir, 0755)
	if err != nil {
		log.Println("Error creating STORAGE directory:", err)
		return
	}

	// Load Manifests
	db.manifest = db.loadManifest()

	// TODO: Discover SST files
	db.discoverSSTs()

	// TODO: Initialize New memtable
	db.activeMem = memtable.NewMemtable(memtable.NewSkipList())

	// Recover WAL
	err = db.replayWAL()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[WAL] Replay complete. DB struct: %+v", db)

	// Open a new wal
	var activeWalID uint64 = 0

	if len(db.walSegments) > 0 {
		activeWalID = db.walSegments[len(db.walSegments)-1].Id + 1
	}

	wdir := filepath.Join(db.storageDir, "wal")

	activeWalPath := filepath.Join(wdir, fmt.Sprintf("wal-%06d.log", activeWalID))
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

	go db.flushMemtableWorker()
}

func (db *DB) IsInitialized() bool {
	return db.isInitialized
}

// TODO: Scan the manifest file and load the SSTs
func (db *DB) discoverSSTs() {
	log.Println("[SST] discovering SST files")

	sstDir := filepath.Join(db.storageDir, "sst")

	err := os.MkdirAll(sstDir, 0755)
	if err != nil {
		log.Fatalln("Error creating SST directory:", err)
		return
	}

	itr := db.manifest.NewIterator()

	var maxSstID int

	for itr.Next() {
		rec := itr.Value()

		log.Printf("[SST] loading rec: %v, level: %v\n", rec.FileID, rec.Level)

		filePath := filepath.Join(sstDir, rec.FileID+".sst")
		fd, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("could not load SST file: %v", err)
		}

		fSize, err := fd.Stat()
		if err != nil {
			log.Fatalf("could not Get SST file size: %v", err)
		}

		sstReader, err := sstable.NewSstReader(fd, fSize.Size())
		if err != nil {
			log.Fatalf("could not create SST file reader: %v", err)
		}

		// Append at the front of the level
		db.levels[rec.Level] = append([]*sstable.SstReader{sstReader}, db.levels[rec.Level]...)

		fileIDNum, err := strconv.Atoi(rec.FileID)
		if err != nil {
			log.Fatalln("Could not convert SST fileID string to int", err)
		}

		maxSstID = max(maxSstID, fileIDNum)
	}

	if itr.Err() != nil && itr.Err() != io.EOF {
		log.Fatalf("error iterating through manifest records: %v", itr.Err())
	}

	db.nextSstID = maxSstID + 1
}

func (db *DB) loadManifest() *manifest {
	log.Println("[MAINFEST] loading")

	mdir := filepath.Join(db.storageDir, "manifest")

	err := os.MkdirAll(mdir, 0755)
	if err != nil {
		log.Fatalln("Error creating MANIFEST directory:", err)
		return nil
	}

	mFilePath := filepath.Join(mdir, "manifest.log")

	fd, err := os.OpenFile(mFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatal(err)
	}

	return &manifest{
		fd: fd,
	}
}

var ErrDatabaseReadonly = errors.New("database in readonly mode due to error")

// TODO:
// Allocate seq number
// append to wal
// insert into active memtable
// if memtable full -> freeze + schedule flush
func (db *DB) Put(userKey, value []byte) (ok bool, err error) {
	if !db.isInitialized {
		log.Fatal("db not initialized. Call db.Open()")
	}

	if db.errState {
		return false, ErrDatabaseReadonly
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

	db.mu.RLock()

	active := db.activeMem
	immutables := db.frozenMems

	db.mu.RUnlock()

	var rec *memtable.Node

	rec, ok = active.Get(userKey, db.nextSeq-1)
	if !ok {
		// Search in frozen memtables
		rec, ok = db.searchFrozenMemtables(immutables, userKey, db.nextSeq-1)

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

	if db.errState {
		return false, ErrDatabaseReadonly
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

	// TODO: Check for memtable flushing
	if db.activeMem.Size() >= db.memtableMaxSize {
		// Flush to SST
		db.freezeActiveMemtable()

		// TODO: Trigger flushing
	}

	return true, err
}

func (db *DB) freezeActiveMemtable() {
	db.mu.Lock()

	// Freeze the current active memtable
	db.activeMem.Freeze()

	frozen := db.activeMem

	// Append the back
	db.frozenMems = append(db.frozenMems, frozen)

	// Create a new active memtable
	db.activeMem = memtable.NewMemtable(memtable.NewSkipList())

	db.mu.Unlock()

	// PUT to flush channel for triggering flushing
	db.flushChan <- frozen
}

// Searches the frozen memtables from newest to oldest
func (db *DB) searchFrozenMemtables(immutables []*memtable.Memtable, userKey []byte, snapshotSeq uint64) (rec *memtable.Node, ok bool) {
	log.Print("[GET] searching in frozen memtable")
	for i := len(immutables) - 1; i >= 0; i-- {
		memtable := immutables[i]

		rec, ok := memtable.Get(userKey, snapshotSeq)
		if ok {
			return rec, ok
		}
	}

	return nil, false
}

func (db *DB) flushMemtableWorker() {
	for mt := range db.flushChan {

		for {
			err := db.flushToSST(mt)
			if err == nil {
				break
			}

			db.mu.Lock()
			db.errState = true
			db.err = err
			db.mu.Unlock()

			log.Println("Flush failed, retrying:", err)
			time.Sleep(2 * time.Second)
		}

		db.mu.Lock()
		db.removeImmutableMemtable()
		db.err = nil
		db.errState = false
		db.mu.Unlock()
	}
}

// Removes the first(oldest) entry from the immutable memtable
func (db *DB) removeImmutableMemtable() {
	db.frozenMems = db.frozenMems[1:]
}

func (db *DB) flushToSST(memtable *memtable.Memtable) error {
	sstDir := filepath.Join(db.storageDir, "sst")
	tempSstFilePath := filepath.Join(sstDir, fmt.Sprintf("%06d.tmp", db.nextSstID))
	finalSstFiletPath := filepath.Join(sstDir, fmt.Sprintf("%06d.sst", db.nextSstID))

	fd, err := os.OpenFile(tempSstFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	sb := sstable.NewSstBuilder(fd, blockSize)

	itr := memtable.NewIterator()

	for itr.Valid() {
		key := itr.Key()
		val := itr.Value()

		if err := sb.Add(key, val); err != nil {
			return err
		}

		itr.Next()
	}

	smKey, lgKey, err := sb.Finish()

	if err != nil {
		return err
	}

	// Sync sst file
	if err := fd.Sync(); err != nil {
		fd.Close()
		return err
	}

	fd.Close()

	// Rename
	if err := os.Rename(tempSstFilePath, finalSstFiletPath); err != nil {
		return err
	}

	// Fsync directory
	dir, err := os.Open(sstDir)
	if err != nil {
		return err
	}

	defer dir.Close()

	if err := dir.Sync(); err != nil {
		return err
	}

	// Update manifest
	db.manifest.Add(ManifestRecord{
		Operation:   Add,
		Level:       0,
		FileID:      fmt.Sprintf("%06d", db.nextSstID),
		SmallestKey: smKey,
		LargestKey:  lgKey,
	})

	// FSync manifest file
	if err := db.manifest.FSync(); err != nil {
		return err
	}

	db.nextSstID++

	return nil
}
