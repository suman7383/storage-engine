package sstable_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
	"github.com/suman7383/storage-engine/sstable"
	testhelper "github.com/suman7383/storage-engine/testhelpers/sstabletest"
)

// makeTestEntries creates a list of test entries.
//
// It creates n entries with keys from "key-0" to "key-(n-1)" and
// values from "value-0" to "value-(n-1)".
func makeTestEntries(n int) []testhelper.Entry {
	entries := make([]testhelper.Entry, n)

	for i := range n {
		entries[i] = testhelper.Entry{
			Key:   internalkey.NewInternalKey(fmt.Appendf(nil, "key-%d", i), uint64(i), op.OpPut),
			Value: fmt.Appendf(nil, "value-%d", i),
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

func createTestSSTIterator(t *testing.T, entryCount int, blockSize int) (*sstable.SstIterator, []testhelper.Entry) {
	entries := makeTestEntries(entryCount)
	reader := testhelper.CreateTestSSTable(t, entries, blockSize)
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
		assertEqualUserKeys(t, iter.Key(), entries[i].Key)
		assertEqualUserValues(t, iter.Value(), entries[i].Value)
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
		assertEqualUserKeys(t, iter.Key(), entries[i].Key)
		assertEqualUserValues(t, iter.Value(), entries[i].Value)
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
	assertEqualUserKeys(t, iter.Key(), entries[0].Key)
	assertEqualUserValues(t, iter.Value(), entries[0].Value)
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
		assertEqualUserKeys(t, iter.Key(), entries[i].Key)
		assertEqualUserValues(t, iter.Value(), entries[i].Value)
		iter.Next()
		i++
	}

	assertEqualInt(t, i, len(entries))

	// Check if Valid() is false
	if iter.Valid() {
		t.Fatalf("Expected Valid() to be false")
	}
}
