package storageengine

import (
	"slices"

	"github.com/suman7383/storage-engine/sstable"
)

type Version struct {
	levels [][]*sstable.SstReader
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
