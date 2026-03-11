package memtable

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/suman7383/storage-engine/internalkey"
	"github.com/suman7383/storage-engine/op"
	"github.com/suman7383/storage-engine/snapshot"
)

func newInternalKey(userKey string, seq uint64, kind op.OpType) internalkey.InternalKey {
	return internalkey.NewInternalKey([]byte(userKey), seq, kind)
}

func newSnapshot(seq uint64) snapshot.Snapshot {
	return snapshot.NewSnapshot(seq)
}

func TestInsertAndSearch(t *testing.T) {
	sl := NewSkipList()

	key := newInternalKey("foo", 1, op.OpPut)
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

	key1 := newInternalKey("foo", 1, op.OpPut)
	key2 := newInternalKey("foo", 2, op.OpPut)

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

	putKey := newInternalKey("foo", 1, op.OpPut)
	delKey := newInternalKey("foo", 2, op.OpDelete)

	sl.Insert(putKey, []byte("v1"))
	sl.Insert(delKey, nil)

	_, found := sl.Search(delKey)
	if found {
		t.Fatalf("expected delete key search to return false")
	}
}

func TestSortedOrder(t *testing.T) {
	sl := NewSkipList()

	keys := []internalkey.InternalKey{
		newInternalKey("d", 1, op.OpPut),
		newInternalKey("a", 1, op.OpPut),
		newInternalKey("c", 1, op.OpPut),
		newInternalKey("b", 1, op.OpPut),
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

func TestSequenceOrdering(t *testing.T) {
	sl := NewSkipList()

	k1 := newInternalKey("a", 1, op.OpPut)
	k2 := newInternalKey("a", 2, op.OpPut)
	k3 := newInternalKey("a", 3, op.OpPut)

	sl.Insert(k1, []byte("v1"))
	sl.Insert(k2, []byte("v2"))
	sl.Insert(k3, []byte("v3"))

	x := sl.head.next[0]

	if x.Key.Seq() != 3 {
		t.Fatalf("expected highest sequence first")
	}
}

func TestRandomStress(t *testing.T) {
	sl := NewSkipList()

	const N = 50000

	for i := 0; i < N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", rand.Intn(N*10)))
		key := internalkey.NewInternalKey(userKey, uint64(i), op.OpPut)
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
// Snapshot Tests
// ---------------------------------

func TestSnapshotReturnsCorrectVersion(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newInternalKey("a", 1, op.OpPut), []byte("v1"))
	sl.Insert(newInternalKey("a", 2, op.OpPut), []byte("v2"))
	sl.Insert(newInternalKey("a", 3, op.OpPut), []byte("v3"))

	lookup := newInternalKey("a", math.MaxUint64>>8, op.OpPut)
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

	sl.Insert(newInternalKey("a", 1, op.OpPut), []byte("v1"))
	sl.Insert(newInternalKey("a", 2, op.OpPut), []byte("v2"))

	lookup := newInternalKey("a", math.MaxUint64>>8, op.OpPut)
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

	sl.Insert(newInternalKey("a", 1, op.OpPut), []byte("v1"))
	sl.Insert(newInternalKey("a", 2, op.OpDelete), nil)

	lookup := newInternalKey("a", math.MaxUint64>>8, op.OpPut)
	snap := newSnapshot(2)

	_, found := sl.SearchWithSnapshot(lookup, snap)
	if found {
		t.Fatalf("expected key to be deleted at snapshot 2")
	}
}

func TestSnapshotBeforeDeleteSeesValue(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newInternalKey("a", 1, op.OpPut), []byte("v1"))
	sl.Insert(newInternalKey("a", 2, op.OpDelete), nil)

	lookup := newInternalKey("a", math.MaxUint64>>8, op.OpPut)
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

	sl.Insert(newInternalKey("a", 1, op.OpPut), []byte("v1"))

	lookup := newInternalKey("a", math.MaxUint64>>8, op.OpPut)
	snap := newSnapshot(0)

	_, found := sl.SearchWithSnapshot(lookup, snap)
	if found {
		t.Fatalf("expected not found for snapshot 0")
	}
}

func TestSnapshotStopsAtDifferentUserKey(t *testing.T) {
	sl := NewSkipList()

	sl.Insert(newInternalKey("a", 1, op.OpPut), []byte("v1"))
	sl.Insert(newInternalKey("b", 2, op.OpPut), []byte("v2"))

	lookup := newInternalKey("a", math.MaxUint64>>8, op.OpPut)
	snap := newSnapshot(10)

	node, found := sl.SearchWithSnapshot(lookup, snap)
	if !found || node.Key.UserKey() == nil {
		t.Fatalf("expected to find key a only")
	}
}

// ---------------------------------
// Benchmark
// ---------------------------------

func BenchmarkInsert(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	keys := make([]internalkey.InternalKey, b.N)
	values := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", i))
		keys[i] = internalkey.NewInternalKey(userKey, uint64(i), op.OpPut)
		values[i] = userKey
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Insert(keys[i], values[i])
	}
}

func BenchmarkSearch(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	const N = 100000

	// preload data
	for i := 0; i < N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", i))
		key := internalkey.NewInternalKey(userKey, uint64(i), op.OpPut)
		sl.Insert(key, userKey)
	}

	// prepare lookup keys
	lookups := make([]internalkey.InternalKey, b.N)
	for i := 0; i < b.N; i++ {
		idx := i % N
		userKey := []byte(fmt.Sprintf("%08d", idx))
		lookups[i] = internalkey.NewInternalKey(userKey, math.MaxUint64>>8, op.OpPut)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Search(lookups[i])
	}
}

func BenchmarkSearchWithSnapshot(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	const N = 10000
	const Versions = 5

	// insert multiple versions per key
	for i := 0; i < N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", i))
		for v := 1; v <= Versions; v++ {
			key := internalkey.NewInternalKey(userKey, uint64(v), op.OpPut)
			sl.Insert(key, []byte("val"))
		}
	}

	snap := snapshot.NewSnapshot(3)

	lookups := make([]internalkey.InternalKey, b.N)
	for i := 0; i < b.N; i++ {
		idx := i % N
		userKey := []byte(fmt.Sprintf("%08d", idx))
		lookups[i] = internalkey.NewInternalKey(userKey, math.MaxUint64>>8, op.OpPut)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.SearchWithSnapshot(lookups[i], snap)
	}
}

func BenchmarkSearchMiss(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	const N = 100000

	for i := 0; i < N; i++ {
		userKey := []byte(fmt.Sprintf("%08d", i))
		key := internalkey.NewInternalKey(userKey, uint64(i), op.OpPut)
		sl.Insert(key, userKey)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		userKey := []byte(fmt.Sprintf("miss%08d", i))
		key := internalkey.NewInternalKey(userKey, math.MaxUint64>>8, op.OpPut)
		sl.Search(key)
	}
}

func BenchmarkSearchHighVersionChurn(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	const Keys = 10000
	const Versions = 20

	for i := 0; i < Keys; i++ {
		userKey := []byte(fmt.Sprintf("%08d", i))
		for v := 1; v <= Versions; v++ {
			key := internalkey.NewInternalKey(userKey, uint64(v), op.OpPut)
			sl.Insert(key, []byte("val"))
		}
	}

	snap := snapshot.NewSnapshot(10)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idx := i % Keys
		userKey := []byte(fmt.Sprintf("%08d", idx))
		lookup := internalkey.NewInternalKey(userKey, math.MaxUint64>>8, op.OpPut)
		sl.SearchWithSnapshot(lookup, snap)
	}
}
