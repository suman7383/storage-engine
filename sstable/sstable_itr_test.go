package sstable

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
)

// entry holds the key-value pair.
type entry struct {
	key   internalkey.InternalKey
	value []byte
}

// For testing create test sstable
//
// Add entries in the sstable and flush it.
// Then, open the sstable and return the sst reader.
func createTestSSTable(t *testing.T, entries []entry, blockSize int) *SstReader {
	// create temporary file path
	tmpPath := t.TempDir() + "/test.sst"

	// Create the sst file
	f, err := os.Create(tmpPath)
	if err != nil {
		t.Fatalf("failed to create sst file: %v", err)
	}

	// Create the sst builder
	builder := NewSstBuilder(f, blockSize)

	// Add entries to the sst
	for _, e := range entries {
		if err := builder.Add(e.key, e.value); err != nil {
			t.Fatalf("failed to add entry: %v", err)
		}
	}

	// Flush the sst
	smK, lgK, err := builder.Finish()
	if err != nil {
		t.Fatalf("failed to flush sst: %v", err)
	}

	fStat, err := f.Stat()
	if err != nil {
		t.Fatalf("failed to get sst file stat: %v", err)
	}

	// Create sst file reader
	reader, err := NewSstReader(f, fStat.Size(), smK, lgK)
	if err != nil {
		t.Fatalf("failed to create sst reader: %v", err)
	}

	return reader
}

// makeTestEntries creates a list of test entries.
//
// It creates n entries with keys from "key-0" to "key-(n-1)" and
// values from "value-0" to "value-(n-1)".
func makeTestEntries(n int) []entry {
	entries := make([]entry, n)

	for i := range n {
		entries[i] = entry{
			key:   internalkey.NewInternalKey(fmt.Appendf(nil, "key-%d", i), uint64(i), op.OpPut),
			value: fmt.Appendf(nil, "value-%d", i),
		}
	}

	return entries
}

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

// assertEqualInt checks if two int values are equal.
func assertEqualInt(t *testing.T, a, b int) {
	if a != b {
		t.Fatalf("Expected %v, got %v", b, a)
	}
}

func createTestSSTIterator(t *testing.T, entryCount int, blockSize int) (*SstIterator, []entry) {
	entries := makeTestEntries(entryCount)
	reader := createTestSSTable(t, entries, blockSize)
	return reader.NewIterator(), entries
}

// TestSstIterator_SingleBlock tests the sst iterator on a single block.
// The entries fit into a single block.
func TestSstIterator_SingleBlock(t *testing.T) {
	// create sst iterator
	iter, entries := createTestSSTIterator(t, 10, 4096)

	// seek to first
	iter.SeekToFirst()

	// iterate over all entries
	i := 0
	for iter.Valid() {
		assertEqualUserKeys(t, iter.Key(), entries[i].key)
		assertEqualUserValues(t, iter.Value(), entries[i].value)
		iter.Next()
		i++
	}

	assertEqualInt(t, i, len(entries))

	// Check if Valid() is false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}

}

// TestSstIterator_MultipleBlocks tests the sst iterator on multiple blocks.
// The entries fit into multiple blocks.
func TestSstIterator_MultipleBlocks(t *testing.T) {
	// create sst iterator
	iter, entries := createTestSSTIterator(t, 1000, 1024)

	// seek to first
	iter.SeekToFirst()

	// iterate over all entries
	i := 0
	for iter.Valid() {
		assertEqualUserKeys(t, iter.Key(), entries[i].key)
		assertEqualUserValues(t, iter.Value(), entries[i].value)
		iter.Next()
		i++
	}

	assertEqualInt(t, i, len(entries))

	// Check if Valid() is false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}
}

// TestSstIterator_SeekToFirst tests the sst iterator with seek to first.
// It first exhausts the sst iterator and then seeks to first.
func TestSstIterator_SeekToFirstAfterExhaustion(t *testing.T) {
	// create sst iterator
	iter, entries := createTestSSTIterator(t, 100, 1024)

	// seek to first
	iter.SeekToFirst()

	// iterate over all entries
	i := 0
	for iter.Valid() {
		iter.Next()
		i++
	}

	// Check if Valid() is false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}

	// seek to first
	iter.SeekToFirst()

	// Verify we at te first key
	if !iter.Valid() {
		t.Fatalf("Expected Valid() to be true")
	}
	assertEqualUserKeys(t, iter.Key(), entries[0].key)
	assertEqualUserValues(t, iter.Value(), entries[0].value)
}

// TestSstIterator_Exhaustion tests the sst iterator with exhaustion.
// It first exhausts the sst iterator and then calls Next() on exhausted iterator.
func TestSstIterator_Exhaustion(t *testing.T) {
	// create sst iterator
	iter, _ := createTestSSTIterator(t, 100, 1024)

	// seek to first
	iter.SeekToFirst()

	// iterate over all entries
	i := 0
	for iter.Valid() {
		iter.Next()
		i++
	}

	// Check if Valid() is false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}

	// call Next() on exhausted iterator
	iter.Next()

	// Verify that Valid() is still false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}
}

// Test single entry in SST
func TestSstIterator_SingleEntry(t *testing.T) {
	// create sst iterator
	iter, entries := createTestSSTIterator(t, 1, 4096)

	// seek to first
	iter.SeekToFirst()

	// iterate over all entries
	i := 0
	for iter.Valid() {
		assertEqualUserKeys(t, iter.Key(), entries[i].key)
		assertEqualUserValues(t, iter.Value(), entries[i].value)
		iter.Next()
		i++
	}

	assertEqualInt(t, i, len(entries))

	// Check if Valid() is false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}
}
