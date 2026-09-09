package storageengine

import (
	"sync/atomic"

	"github.com/suman7383/storage-engine/sstable"
)

type Version struct {
	levels [][]*sstable.SstReader
	refs   atomic.Uint32
}

// Creates a new version with empty levels.
// Initializes the refs count to 1.
func NewVersion() *Version {
	ver := &Version{
		levels: make([][]*sstable.SstReader, 5),
	}

	ver.refs.Store(1)

	return ver
}

// Clone copies the levels of the version 'v',
// therefore copying the *SstReader at each level
// and calls Acquire() on the readers, which increases
// the refs count on the underlying *SstReader.
func (v *Version) Clone() *Version {
	newVersion := NewVersion()

	for i, level := range v.levels {
		newVersion.levels[i] = make([]*sstable.SstReader, len(level))

		for j, reader := range level {
			if reader != nil {
				reader.Acquire()
				newVersion.levels[i][j] = reader
			}
		}
	}

	return newVersion
}

// getCurrentVersion safely reads the currentVersion pointer and returns it.
// Note: It does not create a copy, just returns the pointer. So the caller
// should not mutate the returned version
func (db *DB) getCurrentVersion() *Version {
	db.versionMu.RLock()
	defer db.versionMu.RUnlock()
	return db.currentVersion
}

// installVersion safely installs a new version
// It takes a pointer to the new version and updates currentVersion
func (db *DB) installVersion(v *Version) {
	db.versionMu.Lock()
	defer db.versionMu.Unlock()
	db.currentVersion = v
}

// Get the current version and increment its reference count
// Caller MUST call release on the returned version when done
func (db *DB) AcquireVersion() *Version {
	db.versionMu.RLock()
	defer db.versionMu.RUnlock()

	currentVersion := db.currentVersion
	currentVersion.refs.Add(1)
	return currentVersion
}

// decrement reference count. If refs == 0,
// call Release() on each *SstReader on each level
// to release the ref this version is holding onto it.
func (v *Version) Release() {
	// This basically does -1, since we are adding maxUint32 to it
	// effectively decrementing the reference count without overflow.
	if v.refs.Add(^uint32(0)) == 0 {
		for _, level := range v.levels {
			for _, reader := range level {
				reader.Release()
			}
		}
	}
}
