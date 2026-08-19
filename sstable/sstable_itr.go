package sstable

import (
	"log/slog"

	"github.com/suman7383/storage-engine/internalkey"
)

type SstIterator struct {
	reader        *SstReader
	blockIterator *BlockIterator

	currentBlockIndex int
}

func (si *SstIterator) loadBlock() error {
	block, err := si.reader.ReadBlockAtIndex(si.currentBlockIndex)
	if err != nil {
		return err
	}

	si.blockIterator = block.NewIterator()
	si.blockIterator.SeekToFirst()

	return nil
}

// SeekToFirst initializes the iterator at the first block.
// The Key / Value is set to the first key / value in the first block.
func (si *SstIterator) SeekToFirst() error {
	if si.reader.NumberOfBlocks() == 0 {
		si.blockIterator = nil
		return nil
	}

	si.currentBlockIndex = 0
	return si.loadBlock()
}

// SeekToLast initializes the iterator at the last block.
func (si *SstIterator) SeekToLast() error {
	if si.reader.NumberOfBlocks() == 0 {
		si.blockIterator = nil
		return nil
	}

	si.currentBlockIndex = si.reader.NumberOfBlocks() - 1
	return si.loadBlock()
}

// TODO: SeekToKey initializes the iterator at the block containing the key.
func (si *SstIterator) SeekToKey(key internalkey.InternalKey) error {
	return nil
}

// Next moves the iterator to the next block.
func (si *SstIterator) Next() {
	if si.blockIterator == nil {
		return
	}

	si.blockIterator.Next()

	if si.blockIterator.Valid() {
		return
	}

	// Current block exhausted, move to next block
	slog.Info("Current block exhausted, moving to next block", slog.Int("currentBlockIndex", si.currentBlockIndex))
	si.currentBlockIndex++
	if si.currentBlockIndex >= si.reader.NumberOfBlocks() {
		// SST exhausted. No more blocks to iterate over.
		si.blockIterator = nil
		return
	}

	si.loadBlock()
}

// TODO: Prev moves the iterator to the previous block.
func (si *SstIterator) Prev() {

}

// Valid returns true if the iterator is valid.
func (si *SstIterator) Valid() bool {
	return si.blockIterator != nil && si.blockIterator.Valid()
}

// Key returns the key of the current entry.
func (si *SstIterator) Key() internalkey.InternalKey {
	if !si.Valid() {
		return internalkey.InternalKey{}
	}
	return si.blockIterator.Key()
}

// Value returns the value of the current entry.
func (si *SstIterator) Value() []byte {
	if !si.Valid() {
		return nil
	}
	return si.blockIterator.Value()
}
