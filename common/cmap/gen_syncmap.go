package cmap

import (
	"sync"
)

// Map is a generic, type-safe wrapper around sync.Map. It eliminates the
// boilerplate type assertions required when using sync.Map directly and stores
// a typed zero value so Load can return (V, bool) without extra allocations.
// Written by Claude Code claude-opus-4-6.
type Map[K comparable, V any] struct {
	m    sync.Map
	zero V
}

// Load returns the value stored under key, or the zero value and false if the
// key is not present.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) Load(key K) (V, bool) {
	v, ok := m.m.Load(key)
	if !ok {
		return m.zero, false
	}
	return v.(V), true
}

// Store sets the value for key.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) Store(key K, value V) {
	m.m.Store(key, value)
}

// Reset removes all keys from the map, equivalent to calling Clear on the
// underlying sync.Map.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) Reset() {
	m.m.Clear()
}

// LoadOrStore returns the existing value for key if present; otherwise it
// stores value and returns it. The second return value is true when the key
// already existed prior to the call.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) LoadOrStore(key K, value V) (V, bool) {
	v, ok := m.m.LoadOrStore(key, value)
	if !ok {
		return m.zero, false
	}
	return v.(V), true
}

// LoadAndDelete atomically loads and removes the value for key. It returns the
// value and true if the key existed, or the zero value and false otherwise.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) LoadAndDelete(key K) (V, bool) {
	v, ok := m.m.LoadAndDelete(key)
	if !ok {
		return m.zero, false
	}
	return v.(V), true
}

// Delete removes the value for key. It is a no-op if key is not present.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) Delete(key K) {
	m.m.Delete(key)
}

// Swap stores value under key and returns the previous value. The second return
// value is true when a previous value existed.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	v, ok := m.m.Swap(key, value)
	if !ok {
		return m.zero, false
	}
	return v.(V), true
}

// CompareAndSwap atomically swaps the value for key from old to new if the
// current stored value equals old. Returns true if the swap was performed.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) CompareAndSwap(key K, old, new V) (swapped bool) {
	return m.m.CompareAndSwap(key, old, new)
}

// CompareAndDelete atomically deletes key if the currently stored value equals
// old. Returns true if the deletion occurred.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	return m.m.CompareAndDelete(key, old)
}

// Range calls f sequentially for each key-value pair in the map. Iteration
// stops if f returns false. The map must not be modified during iteration via
// Range; concurrent modifications through other methods are safe.
// Written by Claude Code claude-opus-4-6.
func (m *Map[K, V]) Range(f func(K, V) bool) {
	m.m.Range(
		func(key, value any) bool {
			return f(key.(K), value.(V))
		},
	)
}
