package cmap

import (
	"fmt"
	"sync"
)

const SHARD_COUNT = 32

type Stringer interface {
	fmt.Stringer
	comparable
}

type ConcurrentMap[K comparable, V any] struct {
	shards   []*ConcurrentMapShared[K, V]
	sharding func(key K) uint32
}

// ConcurrentMapShared A "thread" safe string to anything map.
type ConcurrentMapShared[K comparable, V any] struct {
	items        map[K]V
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

func create[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	m := ConcurrentMap[K, V]{
		sharding: sharding,
		shards:   make([]*ConcurrentMapShared[K, V], SHARD_COUNT),
	}
	for i := 0; i < SHARD_COUNT; i++ {
		m.shards[i] = &ConcurrentMapShared[K, V]{items: make(map[K]V)}
	}
	return m
}

// New  Creates a new concurrent map.
func New[V any]() ConcurrentMap[string, V] {
	return create[string, V](fnv32)
}

// NewWithCustomShardingFunction Creates a new concurrent map.
func NewWithCustomShardingFunction[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	return create[K, V](sharding)
}

// GetShard returns shard under given key
func (m ConcurrentMap[K, V]) GetShard(key K) *ConcurrentMapShared[K, V] {
	return m.shards[uint(m.sharding(key))%uint(SHARD_COUNT)]
}

// MSet set multiple keys/values under given
func (m ConcurrentMap[K, V]) MSet(data map[K]V) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Set Sets the given value under the specified key.
func (m ConcurrentMap[K, V]) Set(key K, value V) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Swap the given value under the specified key. return old value
func (m ConcurrentMap[K, V]) Swap(key K, value V) (old V, has bool) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	old, ok := shard.items[key]
	shard.items[key] = value
	shard.Unlock()
	return old, ok
}

// SetIfAbsent Sets the given value under the specified key if no value was associated with it.
func (m ConcurrentMap[K, V]) SetIfAbsent(key K, value V) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcurrentMap[K, V]) Get(key K) (V, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

func (m ConcurrentMap[K, V]) GetOrCreate(key K, new func() V) (V, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.Lock()
	// Get item from shard.
	val, ok := shard.items[key]
	if !ok {
		val = new()
		shard.items[key] = val
	}
	shard.Unlock()
	return val, ok

}

// Count returns the number of elements within the map.
func (m ConcurrentMap[K, V]) Count() int {
	count := 0
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has Looks up an item under specified key
func (m ConcurrentMap[K, V]) Has(key K) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m ConcurrentMap[K, V]) Remove(key K) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb[K any, V any] func(key K, v V, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcurrentMap[K, V]) RemoveCb(key K, cb RemoveCb[K, V]) (V, bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return v, remove
}

// Pop removes an element from the map and returns it
func (m ConcurrentMap[K, V]) Pop(key K) (v V, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap[K, V]) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple Used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple[K comparable, V any] struct {
	Key K
	Val V
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
func (m ConcurrentMap[K, V]) IterBuffered() []Tuple[K, V] {
	tempCount := m.Count()
	buff := make([]Tuple[K, V], 0, tempCount+8)
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m.shards[i]
		shard.RLock()
		for k, v := range shard.items {
			buff = append(buff, Tuple[K, V]{Key: k, Val: v})
		}
		shard.RUnlock()
	}
	return buff
}

// Clear removes all items from map.
func (m ConcurrentMap[K, V]) Clear() {
	for i := 0; i < SHARD_COUNT; i++ {
		shard := m.shards[i]
		shard.RLock()
		clear(shard.items)
		shard.RUnlock()
	}
}

// IterCb Iterator callbacalled for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb[K comparable, V any] func(key K, v V) bool

// IterCb Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcurrentMap[K, V]) IterCb(fn IterCb[K, V]) {
	for idx := range m.shards {
		shard := (m.shards)[idx]
		shard.RLock()
		for key, value := range shard.items {
			if !fn(key, value) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
}

func strfnv32[K fmt.Stringer](key K) uint32 {
	return fnv32(key.String())
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
