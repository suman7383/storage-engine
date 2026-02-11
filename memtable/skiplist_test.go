package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
)

func TestInsertAndSearch(t *testing.T) {
	sl := NewSkipList()

	key := []byte("foo")
	value := []byte("bar")

	inserted := sl.Insert(key, value)
	if !inserted {
		t.Fatalf("expected true for first insert")
	}

	node, found := sl.Search(key)
	if !found {
		t.Fatalf("expected key to be found")
	}

	if !bytes.Equal(node.Value, value) {
		t.Fatalf("expected value %s, got %s", value, node.Value)
	}
}

func TestDuplicateInsertOverwrites(t *testing.T) {
	sl := NewSkipList()

	key := []byte("foo")

	sl.Insert(key, []byte("v1"))
	inserted := sl.Insert(key, []byte("v2"))

	if inserted {
		t.Fatalf("expected false for duplicate insert")
	}

	node, found := sl.Search(key)
	if !found {
		t.Fatalf("expected key to exist")
	}

	if !bytes.Equal(node.Value, []byte("v2")) {
		t.Fatalf("expected overwritten value")
	}
}

func TestSortedOrder(t *testing.T) {
	sl := NewSkipList()

	keys := [][]byte{
		[]byte("d"),
		[]byte("a"),
		[]byte("c"),
		[]byte("b"),
	}

	for _, k := range keys {
		sl.Insert(k, k)
	}

	// Traverse level 0
	x := sl.head.next[0]

	prev := []byte{}
	for x != nil {
		if bytes.Compare(prev, x.Key) > 0 {
			t.Fatalf("keys not sorted: %s before %s", prev, x.Key)
		}
		prev = x.Key
		x = x.next[0]
	}
}

func TestBulkInsertAndSearch(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 1000; i++ {
		key := []byte(string(rune(i)))
		sl.Insert(key, key)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(string(rune(i)))
		_, found := sl.Search(key)
		if !found {
			t.Fatalf("missing key %v", key)
		}
	}
}

func NewSkipListWithRand(r *rand.Rand) *Skiplist {
	sl := NewSkipList()
	sl.rand = r
	return sl
}

func TestDeterministicHeights(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	sl := NewSkipListWithRand(r)

	for i := 0; i < 100; i++ {
		sl.Insert([]byte{byte(i)}, []byte{byte(i)})
	}

	if sl.maxHeight <= 1 {
		t.Fatalf("expected height to grow")
	}
}

func TestNoBrokenLinks(t *testing.T) {
	sl := NewSkipList()

	for i := 0; i < 100; i++ {
		sl.Insert([]byte{byte(i)}, []byte{byte(i)})
	}

	for level := 0; level < sl.maxHeight; level++ {
		x := sl.head

		for x.next[level] != nil {
			if bytes.Compare(x.Key, x.next[level].Key) > 0 {
				t.Fatalf("broken order at level %d", level)
			}
			x = x.next[level]
		}
	}
}

func TestRandomStress(t *testing.T) {
	sl := NewSkipList()

	const N = 50000

	for i := 0; i < N; i++ {
		key := []byte(fmt.Sprintf("%08d", rand.Intn(N*10)))
		sl.Insert(key, key)
	}

	// Verify sorted invariant at level 0
	x := sl.head.next[0]
	var prev []byte

	for x != nil {
		if prev != nil && bytes.Compare(prev, x.Key) > 0 {
			t.Fatalf("list not sorted")
		}
		prev = x.Key
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

func BenchmarkInsert(b *testing.B) {
	b.ReportAllocs()

	sl := NewSkipList()

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("%08d", i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sl.Insert(keys[i], keys[i])
	}
}
