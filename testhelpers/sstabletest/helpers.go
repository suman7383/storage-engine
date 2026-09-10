// Package testhelper provides shared test helpers for building and reading
// SSTable files. It is intended to be imported by _test.go files in any
// package that needs to construct test SSTables without duplicating setup
// logic.
package testhelper

import (
	"fmt"
	"os"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
	"github.com/suman7383/storage-engine/sstable"
)

// Entry holds a key-value pair used when constructing test SSTables.
type Entry struct {
	Key   internalkey.InternalKey
	Value []byte
}

// CreateTestSSTable builds a temporary SSTable from the given entries, flushes
// it to disk, and returns an open SstReader ready for use in tests.
//
// The temporary file is placed inside t.TempDir() and is automatically cleaned
// up when the test finishes.
func CreateTestSSTable(t *testing.T, entries []Entry, blockSize int) *sstable.SstReader {
	t.Helper()

	// Create a temporary file for the SSTable.
	tmpPath := t.TempDir() + "/test.sst"

	f, err := os.Create(tmpPath)
	if err != nil {
		t.Fatalf("sstabletest: failed to create sst file: %v", err)
	}

	// Build the SSTable.
	builder := sstable.NewSstBuilder(f, blockSize)

	for _, e := range entries {
		if err := builder.Add(e.Key, e.Value); err != nil {
			t.Fatalf("sstabletest: failed to add entry: %v", err)
		}
	}

	smK, lgK, err := builder.Finish()
	if err != nil {
		t.Fatalf("sstabletest: failed to finish sst: %v", err)
	}

	fStat, err := f.Stat()
	if err != nil {
		t.Fatalf("sstabletest: failed to stat sst file: %v", err)
	}

	// Open the SSTable for reading.
	reader, err := sstable.NewSstReader(f, fStat.Size(), smK, lgK)
	if err != nil {
		t.Fatalf("sstabletest: failed to create sst reader: %v", err)
	}

	return reader
}

// makeTestEntries creates a list of test entries.
//
// It creates n entries with keys from "key-0" to "key-(n-1)" and
// values from "value-0" to "value-(n-1)".
func MakeTestEntries(n int) []Entry {
	entries := make([]Entry, n)

	for i := range n {
		entries[i] = Entry{
			Key:   internalkey.NewInternalKey(fmt.Appendf(nil, "key-%d", i), uint64(i), op.OpPut),
			Value: fmt.Appendf(nil, "value-%d", i),
		}
	}

	return entries
}
