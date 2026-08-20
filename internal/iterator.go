package internal

import "github.com/suman7383/storage-engine/internalkey"

// Iterator is an interface for iterating over key-value pairs.
type Iterator interface {
	// Next advances the iterator to the next key.
	//
	// After calling Next(), the iterator's state is undefined until the next
	// call to Valid(), Key(), or Value(). If the iterator is already exhausted,
	// this operation is a no-op.
	Next() bool

	// Key returns the current key.
	//
	// If the iterator is not valid, this method returns nil.
	Key() internalkey.InternalKey

	// Value returns the value associated with the current key.
	//
	// If the iterator is not valid, this method returns nil.
	Value() []byte

	// Valid returns true if the iterator is positioned on a valid key-value pair.
	//
	// If Valid() returns false, calls to Next(), Key(), and Value() are undefined
	// except that Valid() itself will continue to return false.
	Valid() bool

	// SeekToFirst positions the iterator at the first key.
	//
	// After calling SeekToFirst(), the iterator is positioned on the first key if
	// one exists, and Valid() returns the result. If no keys exist, Valid()
	// returns false.
	SeekToFirst() bool

	// Close closes the iterator. Further calls to any method on the iterator
	// are undefined.
	Close() error
}
