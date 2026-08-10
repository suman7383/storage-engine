package sstable

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
)

type data struct {
	Key   []byte
	Value []byte
}

// Helper function to write a block with key-value pair
// for testing purposes.
// Binary layout:
// | KeyLen  			| 4 bytes
// | ValueLen 			| 4 bytes
// | InternalKey      	|
// | Value    			|
func writeBlock(data []data) *Block {
	var blockData []byte

	for i, d := range data {
		ik := internalkey.NewInternalKey(d.Key, uint64(i), op.OpPut) // sequence number is i, op is 0 (put)
		b := EncodeEntry(ik, d.Value)

		blockData = append(blockData, b...)
	}

	return &Block{data: blockData}
}

func TestLinearSearchEmptyBlock(t *testing.T) {
	block := writeBlock([]data{})

	ik := internalkey.NewInternalKey([]byte("key1"), 0, op.OpPut)
	value, ok := block.linearSearch(ik)

	if ok {
		t.Fatalf("Expected not to find key, but found value: %s", string(value))
	}
}

func TestLinearSearchSingleEntry(t *testing.T) {
	block := writeBlock([]data{
		{Key: []byte("key1"), Value: []byte("value1")},
	})

	ik := internalkey.NewInternalKey([]byte("key1"), 0, op.OpPut)
	value, ok := block.linearSearch(ik)

	if !ok {
		t.Fatalf("Expected to find key, but did not")
	}

	if string(value) != "value1" {
		t.Fatalf("Expected value 'value1', got '%s'", string(value))
	}

	// Test for a non-existing key
	ik2 := internalkey.NewInternalKey([]byte("key2"), 0, op.OpPut)
	value2, ok2 := block.linearSearch(ik2)

	if ok2 {
		t.Fatalf("Expected not to find key, but found value: %s", string(value2))
	}
}

func TestLinearSearchMultipleEntries(t *testing.T) {
	block := writeBlock([]data{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	})

	// Test for existing keys
	for i := 1; i <= 3; i++ {
		key := []byte("key" + fmt.Sprintf("%d", i))
		expectedValue := "value" + fmt.Sprintf("%d", i)

		ik := internalkey.NewInternalKey(key, 0, op.OpPut)
		value, ok := block.linearSearch(ik)

		if !ok {
			t.Fatalf("Expected to find key %s, but did not", string(key))
		}

		if string(value) != expectedValue {
			t.Fatalf("Expected value '%s', got '%s'", expectedValue, string(value))
		}
	}

	// Test for a non-existing key
	ik2 := internalkey.NewInternalKey([]byte("key4"), 0, op.OpPut)
	value2, ok2 := block.linearSearch(ik2)

	if ok2 {
		t.Fatalf("Expected not to find key, but found value: %s", string(value2))
	}
}

func TestMoveToNextEntry(t *testing.T) {
	data := []data{
		{Key: []byte("a"), Value: []byte("x")},
		{Key: []byte("banana"), Value: []byte("very-long-value")},
		{Key: []byte("cat"), Value: []byte("y")},
		{Key: []byte("elephant"), Value: []byte("another-very-long-value")},
	}

	block := writeBlock(data)

	offset := 0
	for i := 1; i <= len(data); i++ {
		nextOffset, key, value, ok := block.MoveToNextEntry(offset)

		if !ok {
			t.Fatalf("Expected to find next entry, but did not")
		}

		expectedKey := data[i-1].Key
		expectedValue := data[i-1].Value

		if !bytes.Equal(key.UserKey(), []byte(expectedKey)) {
			t.Fatalf("Expected key '%s', got '%s'", expectedKey, string(key.UserKey()))
		}

		if !bytes.Equal(value, []byte(expectedValue)) {
			t.Fatalf("Expected value '%s', got '%s'", expectedValue, string(value))
		}

		offset = nextOffset
	}

	// Test for moving beyond the last entry
	_, key, value, ok := block.MoveToNextEntry(offset)
	if ok {
		t.Fatalf("Expected not to find next entry, but found key: %s, value: %s", string(key), string(value))
	}
}
