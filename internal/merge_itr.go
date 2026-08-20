package internal

import "github.com/suman7383/storage-engine/internalkey"

// MergeIterator merges multiple iterators into a single iterator.
//
// Initially, it uses simple linear search to find the next key.
// Later I should implement min heap to efficiently merge the iterators.
//
// The iterators must individually produce keys in sorted order.
// It is not concurrency safe. Ensure only single goroutine is accessing
// this iterator at any given time.
type MergeIterator struct {
	iterators []Iterator
	current   int
}

func NewMergeIterator(iterators []Iterator) *MergeIterator {
	return &MergeIterator{
		iterators: iterators,
		current:   -1,
	}
}

// Next moves to the next key.
// It uses linear search to find the next key.
func (m *MergeIterator) Next() {
	if m.current == -1 {
		// not initialized, so we return
		return
	}

	// Move the iterator pointed by current to the next key
	m.iterators[m.current].Next()

	// find the index of the iterator containing the next smallest key
	// move the pointer of that iterator to the next key
	m.linearNext()
}

// selectFirstValidIterator selects the first valid iterator.
// It returns true if there is a valid iterator,
// and false if there is no valid iterator.
func (m *MergeIterator) selectFirstValidIterator() bool {
	for i := 0; i < len(m.iterators); i++ {
		if m.iterators[i].Valid() {
			m.current = i
			return true
		}
	}

	m.current = -1
	return false
}

// linearNext implements the linear search algorithm to find the index of
// the iterator containing the next smallest key.
//
// After this function is called, m.current will point to the index of the
// iterator containing the next smallest key.
func (m *MergeIterator) linearNext() {
	// If the current iterator is not valid, move to the next valid iterator
	if m.current == -1 || !m.iterators[m.current].Valid() {
		if !m.selectFirstValidIterator() {
			// if no valid iterator is found, return
			return
		}
	}

	// set to index of the iterator containing the next smallest key
	for i, itr := range m.iterators {
		if i != m.current && itr.Valid() && itr.Key().IsLessThan(m.iterators[m.current].Key()) {
			m.current = i
		}
	}
}

// Key returns the current smallest key.
func (m *MergeIterator) Key() internalkey.InternalKey {
	if m.current == -1 || !m.iterators[m.current].Valid() {
		return nil
	}
	return m.iterators[m.current].Key()
}

// Value returns the current value.
func (m *MergeIterator) Value() []byte {
	if m.current == -1 || !m.iterators[m.current].Valid() {
		return nil
	}

	return m.iterators[m.current].Value()
}

// Valid returns true if the iterator is valid.
func (m *MergeIterator) Valid() bool {
	return m.current >= 0 && m.current < len(m.iterators) && m.iterators[m.current].Valid()
}

// SeekToFirst seeks to the first key.
// It calls SeekToFirst on all iterators and then finds the first smallest key.
func (m *MergeIterator) SeekToFirst() bool {
	for _, itr := range m.iterators {
		itr.SeekToFirst()
	}
	m.linearNext()
	return m.Valid()
}

// Close closes the iterator.
func (m *MergeIterator) Close() {
	for _, itr := range m.iterators {
		itr.Close()
	}
}
