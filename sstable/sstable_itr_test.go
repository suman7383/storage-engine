package sstable

import (
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
			key:   internalkey.NewInternalKey([]byte(fmt.Sprintf("key-%d", i)), uint64(i), op.OpPut),
			value: []byte(fmt.Sprintf("value-%d", i)),
		}
	}

	return entries
}
