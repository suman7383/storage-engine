package memtable

import "github.com/suman7383/storage-engine/internalkey"

type SkiplistIterator struct {
	curr *Node
}

func (sli *SkiplistIterator) Valid() bool {
	return sli.curr != nil
}

func (sli *SkiplistIterator) Next() {
	if sli.curr != nil {
		sli.curr = sli.curr.next[0]
	}
}

func (sli *SkiplistIterator) Key() internalkey.InternalKey {
	if sli.curr != nil {

		return sli.curr.Key
	}

	return nil
}

func (sli *SkiplistIterator) Value() []byte {
	if sli.curr != nil {

		return sli.curr.Value
	}

	return nil
}
