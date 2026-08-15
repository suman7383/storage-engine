package sstable

import (
	"bytes"
	"testing"
)

func TestEmptyBlockIterator(t *testing.T) {
	block := writeBlock([]data{})
	iterator := block.NewIterator()

	// Valid should initially be false
	if iterator.Valid() {
		t.Fatalf("Expected iterator to be invalid for an empty block, but it is valid")
	}

	if iterator.Next() {
		t.Fatalf("Expected no entries in the block, but found one")
	}

	if iterator.Valid() {
		t.Fatalf("Expected iterator to be invalid, but it is valid")
	}

	if iterator.Key() != nil {
		t.Fatalf("Expected no key for an invalid iterator, but got: %v", iterator.Key())
	}

	if iterator.Value() != nil {
		t.Fatalf("Expected no value for an invalid iterator, but got: %v", iterator.Value())
	}
}

func TestSingleEntryBlockIterator(t *testing.T) {
	block := writeBlock([]data{
		{Key: []byte("key1"), Value: []byte("value1")},
	})
	iterator := block.NewIterator()

	if !iterator.Next() {
		t.Fatalf("Expected to find an entry in the block, but did not")
	}

	if !iterator.Valid() {
		t.Fatalf("Expected iterator to be valid after Next(), but it is invalid")
	}

	expectedKey := []byte("key1")
	expectedValue := []byte("value1")

	if !bytes.Equal(iterator.Key().UserKey(), expectedKey) {
		t.Fatalf("Expected key '%s', got '%s'", expectedKey, iterator.Key().UserKey())
	}

	if !bytes.Equal(iterator.Value(), expectedValue) {
		t.Fatalf("Expected value '%s', got '%s'", expectedValue, iterator.Value())
	}

	if iterator.Next() {
		t.Fatalf("Expected no more entries in the block, but found one")
	}

	if iterator.Valid() {
		t.Fatalf("Expected iterator to be invalid after reaching the end, but it is valid")
	}
}

func TestMultipleEntriesBlockIterator(t *testing.T) {
	data := []data{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	block := writeBlock(data)
	iterator := block.NewIterator()

	for i := 1; i <= len(data); i++ {
		if !iterator.Next() {
			t.Fatalf("Expected to find entry %d in the block, but did not", i)
		}

		if !iterator.Valid() {
			t.Fatalf("Expected iterator to be valid after Next(), but it is invalid")
		}

		if !bytes.Equal(iterator.Key().UserKey(), data[i-1].Key) {
			t.Fatalf("Expected key '%s', got '%s'", data[i-1].Key, iterator.Key().UserKey())
		}

		if !bytes.Equal(iterator.Value(), data[i-1].Value) {
			t.Fatalf("Expected value '%s', got '%s'", data[i-1].Value, iterator.Value())
		}
	}

	if iterator.Next() {
		t.Fatalf("Expected no more entries in the block, but found one")
	}

	if iterator.Valid() {
		t.Fatalf("Expected iterator to be invalid after reaching the end, but it is valid")
	}
}

func TestSeekToFirst(t *testing.T) {
	data := []data{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	block := writeBlock(data)
	iterator := block.NewIterator()

	// Exhaust the iterator to the end
	for iterator.Next() {
	}

	// Now seek to first
	if !iterator.SeekToFirst() {
		t.Fatalf("Expected to seek to the first entry, but did not")
	}

	if !iterator.Valid() {
		t.Fatalf("Expected iterator to be valid after SeekToFirst(), but it is invalid")
	}

	expectedKey := data[0].Key
	expectedValue := data[0].Value

	if !bytes.Equal(iterator.Key().UserKey(), expectedKey) {
		t.Fatalf("Expected key '%s', got '%s'", expectedKey, iterator.Key().UserKey())
	}

	if !bytes.Equal(iterator.Value(), expectedValue) {
		t.Fatalf("Expected value '%s', got '%s'", expectedValue, iterator.Value())
	}
}
