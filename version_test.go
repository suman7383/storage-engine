package storageengine

import (
	"bytes"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/sstable"
	testhelper "github.com/suman7383/storage-engine/testhelpers/sstabletest"
)

// assertEqual checks if two values are equal.
func assertEqualUserKeys(t *testing.T, a, b internalkey.InternalKey) {
	if !bytes.Equal(a.UserKey(), b.UserKey()) {
		t.Fatalf("Expected %v, got %v", b.UserKey(), a.UserKey())
	}
}

// assertEqualUserValues checks if two byte slices are equal.
func assertEqualUserValues(t *testing.T, a, b []byte) {
	if !bytes.Equal(a, b) {
		t.Fatalf("Expected %v, got %v", b, a)
	}
}

func TestVersion_Clone(t *testing.T) {
	v := NewVersion()

	entries := testhelper.MakeTestEntries(5)

	reader := testhelper.CreateTestSSTable(t, entries, 4096)

	// Add the readers to level 0 of v
	v.levels[0] = []*sstable.SstReader{reader}

	// Clone the version
	clonedVersion := v.Clone()

	// reader now has 2 owners
	// test if we can read the data from the cloned version
	// for all the entries
	level0 := clonedVersion.levels[0][0] // cloned version reader
	iter := level0.NewIterator()

	i := 0
	for iter.Valid() {
		key, value := iter.Key(), iter.Value()

		assertEqualUserKeys(t, key, entries[i].Key)
		assertEqualUserValues(t, value, entries[i].Value)
		iter.Next()
		i++
	}

	// Release original version
	v.Release()

	// Check if we can still read from the cloned version
	level0 = clonedVersion.levels[0][0]
	iter = level0.NewIterator()

	i = 0
	for iter.Valid() {
		key, value := iter.Key(), iter.Value()

		assertEqualUserKeys(t, key, entries[i].Key)
		assertEqualUserValues(t, value, entries[i].Value)
		iter.Next()
		i++
	}

	// Release cloned version
	clonedVersion.Release()

	// The sstreader's fd should now be closed
	// since there is no version referencing it
	// We can test this by checking the closed flag
	if !clonedVersion.levels[0][0].IsClosed {
		t.Fatalf("Expected sstreader to be closed")
	}
}
