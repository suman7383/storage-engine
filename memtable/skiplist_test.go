package memtable

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/snapshot"
)

func TestInsertAndSearch(t *testing.T) {
	sl := NewSkipList()

	key := internalkey.NewKey([]byte("foo"), 1, internalkey.KeyPut)
	value := []byte("bar")

	sl.Insert(key, value)

	node, found := sl.Search(key)
	if !found {
		t.Fatalf("expected key to be found")
	}

	if !bytes.Equal(node.Value, value) {
		t.Fatalf("expected value %s, got %s", value, node.Value)
	}
}

func TestMultipleVersionsCoexist(t *testing.T) {
	sl := NewSkipList()

	key1 := internalkey.NewKey([]byte("foo"), 1, internalkey.KeyPut)
	key2 := internalkey.NewKey([]byte("foo"), 2, internalkey.KeyPut)

	sl.Insert(key1, []byte("v1"))
	sl.Insert(key2, []byte("v2"))

	_, found1 := sl.Search(key1)
	_, found2 := sl.Search(key2)

	if !found1 || !found2 {
		t.Fatalf("expected both versions to exist")
	}
}

func TestDeleteEntry(t *testing.T) {
	sl := NewSkipList()

	putKey := internalkey.NewKey([]byte("foo"), 1, internalkey.KeyPut)
	delKey := internalkey.NewKey([]byte("foo"), 2, internalkey.KeyDelete)

	sl.Insert(putKey, []byte("v1"))
	sl.Insert(delKey, nil)

	_, found := sl.Search(delKey)
	if found {
		t.Fatalf("expected delete key search to return false")
	}
}

func TestSortedOrder(t *testing.T) {
	sl := NewSkipList()

	keys := []internalkey.Key{
		internalkey.NewKey([]byte("d"), 1, internalkey.KeyPut),
		internalkey.NewKey([]byte("a"), 1, internalkey.KeyPut),
		internalkey.NewKey([]byte("c"), 1, internalkey.KeyPut),
		internalkey.NewKey([]byte("b"), 1, internalkey.KeyPut),
	}

	for _, k := range keys {
		sl.Insert(k, []byte("val"))
	}

	x := sl.head.next[0]
	var prev *Node

	for x != nil {
		if prev != nil && prev.Key.Compare(x.Key) > 0 {
			t.Fatalf("keys not sorted")
		}
		prev = x
		x = x.next[0]
	}
}

func NewSkipListWithRand(r *rand.Rand) *Skiplist {
	sl := NewSkipList()
	sl.rand = r
	return sl
}

func TestSequenceOrdering(t *testing.T) {
	sl := NewSkipList()

	k1 := internalkey.NewKey([]byte("a"), 1, internalkey.KeyPut)
	k2 := internalkey.NewKey([]byte("a"), 2, internalkey.KeyPut)
	k3 := internalkey.NewKey([]byte("a"), 3, internalkey.KeyPut)

	sl.Insert(k1, []byte("v1"))
	sl.Insert(k2, []byte("v2"))
	sl.Insert(k3, []byte("v3"))

	x := sl.head.next[0]

	// First should be highest seq
	if x.Key.Seq() != 3 {
		t.Fatalf("expected highest sequence first")
	}
}

func TestRandomStress(t *testing.T) {
	sl := NewSkipList()

	const N = 50000

	for i := 0; i < N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", rand.Intn(N*10)))
		key := internalkey.NewKey(userKey, uint64(i), internalkey.KeyPut)
		sl.Insert(key, userKey)
	}

	x := sl.head.next[0]
	var prev *Node

	for x != nil {
		if prev != nil && prev.Key.Compare(x.Key) > 0 {
			t.Fatalf("list not sorted")
		}
		prev = x
		x = x.next[0]
	}
}

func TestHeightDistribution(t *testing.T) {
	sl := NewSkipList()

	const N = 200000
	count := make([]int, MAX_LEVEL)

	for i := 0; i < N; i++ {
		h := sl.randomHeight()
		count[h-1]++
	}

	// Check average height ~2
	total := 0
	for i := 0; i < MAX_LEVEL; i++ {
		total += count[i] * (i + 1)
	}

	avg := float64(total) / float64(N)

	if avg < 1.8 || avg > 2.2 {
		t.Fatalf("unexpected average height: %f", avg)
	}
}

// ---------------------------------
//		Test with snapshot
// ---------------------------------

func newKey(userKey string, seq uint64, kind internalkey.KeyType) internalkey.Key {
	return internalkey.NewKey([]byte(userKey), seq, kind)
}

func newSnapshot(seq uint64) snapshot.Snapshot {
	return snapshot.NewSnapshot(seq)
}

func TestSnapshotReturnsCorrectVersion(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newKey("a", 1, internalkey.KeyPut), []byte("v1"))
	sl.Insert(newKey("a", 2, internalkey.KeyPut), []byte("v2"))
	sl.Insert(newKey("a", 3, internalkey.KeyPut), []byte("v3"))

	lookup := newKey("a", math.MaxUint64, internalkey.KeyPut)
	snap := newSnapshot(2)

	node, found := sl.SearchWithSnapshot(lookup, snap)
	if !found {
		t.Fatalf("expected version at seq 2")
	}

	if node.Key.Seq() != 2 {
		t.Fatalf("expected seq 2, got %d", node.Key.Seq())
	}
}

func TestSnapshotSkipsNewerVersions(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newKey("a", 1, internalkey.KeyPut), []byte("v1"))
	sl.Insert(newKey("a", 2, internalkey.KeyPut), []byte("v2"))

	lookup := newKey("a", math.MaxUint64, internalkey.KeyPut)
	snap := newSnapshot(1)

	node, found := sl.SearchWithSnapshot(lookup, snap)
	if !found {
		t.Fatalf("expected version at seq 1")
	}

	if node.Key.Seq() != 1 {
		t.Fatalf("expected seq 1, got %d", node.Key.Seq())
	}
}

func TestSnapshotRespectsDelete(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newKey("a", 1, internalkey.KeyPut), []byte("v1"))
	sl.Insert(newKey("a", 2, internalkey.KeyDelete), nil)

	lookup := newKey("a", math.MaxUint64, internalkey.KeyPut)
	snap := newSnapshot(2)

	_, found := sl.SearchWithSnapshot(lookup, snap)
	if found {
		t.Fatalf("expected key to be deleted at snapshot 2")
	}
}

func TestSnapshotBeforeDeleteSeesValue(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newKey("a", 1, internalkey.KeyPut), []byte("v1"))
	sl.Insert(newKey("a", 2, internalkey.KeyDelete), nil)

	lookup := newKey("a", math.MaxUint64, internalkey.KeyPut)
	snap := newSnapshot(1)

	node, found := sl.SearchWithSnapshot(lookup, snap)
	if !found {
		t.Fatalf("expected value at snapshot 1")
	}

	if node.Key.Seq() != 1 {
		t.Fatalf("expected seq 1, got %d", node.Key.Seq())
	}
}

func TestSnapshotBeforeAnyWrite(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newKey("a", 1, internalkey.KeyPut), []byte("v1"))

	lookup := newKey("a", math.MaxUint64, internalkey.KeyPut)
	snap := newSnapshot(0)

	_, found := sl.SearchWithSnapshot(lookup, snap)
	if found {
		t.Fatalf("expected not found for snapshot 0")
	}
}

func TestSnapshotStopsAtDifferentUserKey(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newKey("a", 1, internalkey.KeyPut), []byte("v1"))
	sl.Insert(newKey("b", 2, internalkey.KeyPut), []byte("v2"))

	lookup := newKey("a", math.MaxUint64, internalkey.KeyPut)
	snap := newSnapshot(10)

	node, found := sl.SearchWithSnapshot(lookup, snap)
	if !found || node.Key.UserKey() == nil {
		t.Fatalf("expected to find key a only")
	}
}

//------------------------------------------
// 			BENCHMARK
// -----------------------------------------

func BenchmarkInsert(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	keys := make([]internalkey.Key, b.N)
	values := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", i))
		keys[i] = internalkey.NewKey(userKey, uint64(i), internalkey.KeyPut)
		values[i] = userKey
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Insert(keys[i], values[i])
	}
}
