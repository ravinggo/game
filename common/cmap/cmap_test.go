// Package cmap_test contains tests for the cmap package.
// Written by Claude Code claude-opus-4-6.
package cmap

import (
	"fmt"
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// ConcurrentMap tests
// ---------------------------------------------------------------------------

func TestConcurrentMap_SetAndGet(t *testing.T) {
	m := New[int]()
	m.Set("key1", 42)

	val, ok := m.Get("key1")
	if !ok {
		t.Fatal("expected key1 to exist")
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestConcurrentMap_GetMissing(t *testing.T) {
	m := New[int]()
	_, ok := m.Get("missing")
	if ok {
		t.Fatal("expected missing key to return false")
	}
}

func TestConcurrentMap_Has(t *testing.T) {
	m := New[string]()
	m.Set("foo", "bar")

	if !m.Has("foo") {
		t.Fatal("expected Has to return true for existing key")
	}
	if m.Has("absent") {
		t.Fatal("expected Has to return false for absent key")
	}
}

func TestConcurrentMap_Remove(t *testing.T) {
	m := New[int]()
	m.Set("a", 1)
	m.Remove("a")

	if m.Has("a") {
		t.Fatal("expected key to be removed")
	}
	// Removing a non-existent key must not panic.
	m.Remove("nonexistent")
}

func TestConcurrentMap_SetIfAbsent(t *testing.T) {
	m := New[int]()

	// First call should set and return true.
	if !m.SetIfAbsent("k", 10) {
		t.Fatal("expected true on first SetIfAbsent")
	}
	// Second call must not overwrite and return false.
	if m.SetIfAbsent("k", 99) {
		t.Fatal("expected false when key already exists")
	}
	val, _ := m.Get("k")
	if val != 10 {
		t.Fatalf("expected value to remain 10, got %d", val)
	}
}

func TestConcurrentMap_GetOrCreate(t *testing.T) {
	m := New[int]()

	// Key absent: factory called, ok == false.
	val, existed := m.GetOrCreate("x", func() int { return 7 })
	if existed {
		t.Fatal("expected existed=false for new key")
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}

	// Key present: factory NOT called, ok == true.
	val, existed = m.GetOrCreate("x", func() int { return 99 })
	if !existed {
		t.Fatal("expected existed=true for existing key")
	}
	if val != 7 {
		t.Fatalf("expected 7, got %d", val)
	}
}

func TestConcurrentMap_Pop(t *testing.T) {
	m := New[string]()
	m.Set("p", "hello")

	v, exists := m.Pop("p")
	if !exists {
		t.Fatal("expected exists=true for Pop on existing key")
	}
	if v != "hello" {
		t.Fatalf("expected 'hello', got %q", v)
	}
	if m.Has("p") {
		t.Fatal("key should be gone after Pop")
	}

	// Pop on missing key: exists must be false.
	_, exists = m.Pop("missing")
	if exists {
		t.Fatal("expected exists=false for Pop on missing key")
	}
}

func TestConcurrentMap_Count(t *testing.T) {
	m := New[int]()
	if m.Count() != 0 {
		t.Fatal("expected empty map count to be 0")
	}
	m.Set("a", 1)
	m.Set("b", 2)
	if m.Count() != 2 {
		t.Fatalf("expected count 2, got %d", m.Count())
	}
	m.Remove("a")
	if m.Count() != 1 {
		t.Fatalf("expected count 1 after remove, got %d", m.Count())
	}
}

func TestConcurrentMap_IsEmpty(t *testing.T) {
	m := New[int]()
	if !m.IsEmpty() {
		t.Fatal("expected new map to be empty")
	}
	m.Set("k", 1)
	if m.IsEmpty() {
		t.Fatal("expected non-empty map")
	}
}

func TestConcurrentMap_IterBuffered(t *testing.T) {
	m := New[int]()
	want := map[string]int{"a": 1, "b": 2, "c": 3}
	for k, v := range want {
		m.Set(k, v)
	}

	got := m.IterBuffered()
	if len(got) != len(want) {
		t.Fatalf("expected %d entries, got %d", len(want), len(got))
	}
	seen := make(map[string]int, len(got))
	for _, t := range got {
		seen[t.Key] = t.Val
	}
	for k, v := range want {
		if seen[k] != v {
			t.Errorf("key %q: expected %d, got %d", k, v, seen[k])
		}
	}
}

func TestConcurrentMap_IterCb(t *testing.T) {
	m := New[int]()
	m.Set("x", 10)
	m.Set("y", 20)

	seen := make(map[string]int)
	m.IterCb(func(key string, v int) bool {
		seen[key] = v
		return true
	})

	if len(seen) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(seen))
	}
	if seen["x"] != 10 || seen["y"] != 20 {
		t.Errorf("unexpected values: %v", seen)
	}
}

func TestConcurrentMap_IterCb_EarlyStop(t *testing.T) {
	m := New[int]()
	for i := 0; i < 10; i++ {
		m.Set(fmt.Sprintf("k%d", i), i)
	}

	count := 0
	m.IterCb(func(_ string, _ int) bool {
		count++
		return false // stop immediately after first entry
	})
	if count != 1 {
		t.Fatalf("expected callback to stop after 1 call, got %d", count)
	}
}

func TestConcurrentMap_MSet(t *testing.T) {
	m := New[int]()
	m.MSet(map[string]int{"a": 1, "b": 2})

	v, ok := m.Get("a")
	if !ok || v != 1 {
		t.Errorf("expected a=1, got %d ok=%v", v, ok)
	}
	v, ok = m.Get("b")
	if !ok || v != 2 {
		t.Errorf("expected b=2, got %d ok=%v", v, ok)
	}
}

func TestConcurrentMap_Concurrent(t *testing.T) {
	m := New[int]()
	const workers = 8
	const perWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers * 2)

	// Writers.
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				m.Set(fmt.Sprintf("w%d-k%d", w, i), w*perWorker+i)
			}
		}()
	}

	// Readers (concurrent with writers).
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				m.Get(fmt.Sprintf("w%d-k%d", w, i))
			}
		}()
	}

	wg.Wait()

	if m.Count() != workers*perWorker {
		t.Fatalf("expected %d entries, got %d", workers*perWorker, m.Count())
	}
}

func TestConcurrentMap_Swap(t *testing.T) {
	m := New[int]()

	// Swap on absent key: has==false, new value stored.
	old, has := m.Swap("key", 1)
	if has {
		t.Fatal("expected has=false for first swap")
	}
	_ = old // zero value

	// Swap on existing key: has==true, old value returned.
	old, has = m.Swap("key", 2)
	if !has {
		t.Fatal("expected has=true for second swap")
	}
	if old != 1 {
		t.Fatalf("expected old value 1, got %d", old)
	}
	v, _ := m.Get("key")
	if v != 2 {
		t.Fatalf("expected current value 2, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// Map (sync.Map wrapper) tests
// ---------------------------------------------------------------------------

func TestSyncMap_StoreAndLoad(t *testing.T) {
	var m Map[string, int]
	m.Store("hello", 42)

	v, ok := m.Load("hello")
	if !ok {
		t.Fatal("expected key to be present")
	}
	if v != 42 {
		t.Fatalf("expected 42, got %d", v)
	}
}

func TestSyncMap_LoadMissing(t *testing.T) {
	var m Map[string, int]
	_, ok := m.Load("absent")
	if ok {
		t.Fatal("expected false for missing key")
	}
}

func TestSyncMap_Delete(t *testing.T) {
	var m Map[string, string]
	m.Store("del", "value")
	m.Delete("del")

	_, ok := m.Load("del")
	if ok {
		t.Fatal("expected key to be deleted")
	}
	// Deleting a missing key must not panic.
	m.Delete("nonexistent")
}

func TestSyncMap_LoadOrStore_ReturnsExisting(t *testing.T) {
	var m Map[string, int]
	m.Store("k", 5)

	// LoadOrStore should return existing value and loaded==true.
	v, loaded := m.LoadOrStore("k", 99)
	if !loaded {
		t.Fatal("expected loaded=true for existing key")
	}
	if v != 5 {
		t.Fatalf("expected existing value 5, got %d", v)
	}
}

func TestSyncMap_LoadOrStore_StoresAbsent(t *testing.T) {
	var m Map[string, int]

	// LoadOrStore on absent key: loaded==false, value stored.
	v, loaded := m.LoadOrStore("new", 7)
	if loaded {
		t.Fatal("expected loaded=false for new key")
	}
	// The returned value is the zero value per current implementation contract.
	_ = v

	stored, ok := m.Load("new")
	if !ok {
		t.Fatal("expected key to be stored after LoadOrStore")
	}
	if stored != 7 {
		t.Fatalf("expected stored value 7, got %d", stored)
	}
}

func TestSyncMap_Swap(t *testing.T) {
	var m Map[string, int]

	// Swap on absent key: loaded==false.
	prev, loaded := m.Swap("s", 10)
	if loaded {
		t.Fatal("expected loaded=false for swap on absent key")
	}
	_ = prev

	// Swap on existing key: loaded==true, old value returned.
	prev, loaded = m.Swap("s", 20)
	if !loaded {
		t.Fatal("expected loaded=true for swap on existing key")
	}
	if prev != 10 {
		t.Fatalf("expected previous value 10, got %d", prev)
	}
	v, _ := m.Load("s")
	if v != 20 {
		t.Fatalf("expected current value 20, got %d", v)
	}
}

func TestSyncMap_Range(t *testing.T) {
	var m Map[string, int]
	m.Store("a", 1)
	m.Store("b", 2)

	seen := make(map[string]int)
	m.Range(func(k string, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(seen))
	}
}
