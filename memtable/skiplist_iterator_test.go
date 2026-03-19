package memtable

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
)

func TestSkiplistIteratorSortedOrder(t *testing.T) {
	sl := NewSkipList()

	const totalKeys = 2700

	r := rand.New(rand.NewSource(42)) // deterministic

	var keys []internalkey.InternalKey

	// Generate keys
	for i := 0; i < totalKeys; i++ {
		userKey := []byte(fmt.Sprintf("key-%06d", i))
		seq := uint64(r.Intn(1000))

		k := internalkey.NewInternalKey(userKey, seq, op.OpPut)
		keys = append(keys, k)
	}

	// Shuffle insertion order
	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// Insert
	for _, k := range keys {
		sl.Insert(k, []byte("value"))
	}

	// Iterate + validate order
	iter := sl.NewIterator()

	var prev internalkey.InternalKey
	first := true
	count := 0

	for iter.Valid() {
		curr := iter.Key()

		if !first {
			if prev.Compare(curr) > 0 {
				t.Fatalf("iterator order violated:\nprev: %v\ncurr: %v", prev, curr)
			}
		}

		prev = curr
		first = false
		count++

		iter.Next()
	}

	if count != totalKeys {
		t.Fatalf("expected %d keys, got %d", totalKeys, count)
	}
}

func TestSkiplistIteratorInternalKeyOrdering(t *testing.T) {
	sl := NewSkipList()

	const totalKeys = 2700

	r := rand.New(rand.NewSource(99))

	var keys []internalkey.InternalKey

	// Only 100 unique userKeys → forces collisions
	for i := 0; i < totalKeys; i++ {
		userKey := []byte(fmt.Sprintf("key-%03d", i%100))
		seq := uint64(i) // strictly increasing

		k := internalkey.NewInternalKey(userKey, seq, op.OpPut)
		keys = append(keys, k)
	}

	// Shuffle before insert
	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for _, k := range keys {
		sl.Insert(k, []byte("value"))
	}

	iter := sl.NewIterator()

	var prev internalkey.InternalKey
	first := true

	for iter.Valid() {
		curr := iter.Key()

		if !first {
			userCmp := prev.CompareUserKeys(curr)

			if userCmp == 0 {
				// Same userKey → seq must be DESC
				if prev.Seq() < curr.Seq() {
					t.Fatalf("sequence order violated for same userKey:\nprev: %v\ncurr: %v", prev, curr)
				}
			} else if userCmp > 0 {
				t.Fatalf("userKey order violated:\nprev: %v\ncurr: %v", prev, curr)
			}
		}

		prev = curr
		first = false
		iter.Next()
	}
}
