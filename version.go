package storageengine

import (
	"slices"
	"sync/atomic"

	"github.com/suman7383/storage-engine/sstable"
)

type Version struct {
	levels [][]*sstable.SstReader
	refs   atomic.Uint32
}

// Creates a new version with empty levels
func NewVersion() *Version {
	return &Version{
		levels: make([][]*sstable.SstReader, 5),
	}
}

func (v *Version) Clone() *Version {
	newVersion := NewVersion()

	for level := range v.levels {
		newVersion.levels[level] = slices.Clone(v.levels[level])
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

// decrement reference count and free if 0
func (v *Version) Release() {
	// This basically does -1, since we are adding maxUint32 to it
	// effectively decrementing the reference count without overflow.
	v.refs.Add(^uint32(0))
}
