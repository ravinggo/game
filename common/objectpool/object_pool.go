package objectpool

import (
	"math"
	"math/bits"
	"sync"
	"unsafe"
)

const (
	maxIndex    = math.MaxUint16 - 1
	otherMinCap = 16
	byteMinCap  = 128
)

var (
	op       = objectPool{}
	bytesPtr = func() uintptr {
		var a interface{} = (*Slice[byte])(nil)
		return *(*uintptr)(unsafe.Pointer(&a))
	}()
)

type poolUintptr struct {
	uintptr
	singlePool *sync.Pool
	slicePool  *slicePool
}

type objectPool struct {
	m  [math.MaxUint16][]*poolUintptr
	ml [math.MaxUint16]sync.Mutex
}

func (o *objectPool) get(p uintptr) *sync.Pool {
	index := (p >> 6) & maxIndex

	ss := o.m[index]
	for _, s := range ss {
		if s.uintptr == p {
			return s.singlePool
		}
	}

	// lock for index conflict,
	o.ml[index].Lock()
	for _, s := range o.m[index] {
		if s.uintptr == p {
			o.ml[index].Unlock()
			return s.singlePool
		}
	}
	pu := &poolUintptr{
		uintptr:    p,
		singlePool: &sync.Pool{},
	}
	o.m[index] = append(o.m[index], pu)
	o.ml[index].Unlock()
	return pu.singlePool
}

func (o *objectPool) getSlice(p uintptr) *slicePool {
	index := (p >> 6) & maxIndex

	ss := o.m[index]
	for _, s := range ss {
		if s.uintptr == p {
			return s.slicePool
		}
	}

	// lock for index conflict,
	o.ml[index].Lock()
	for _, s := range o.m[index] {
		if s.uintptr == p {
			o.ml[index].Unlock()
			return s.slicePool
		}
	}
	pu := &poolUintptr{
		uintptr:   p,
		slicePool: &slicePool{},
	}
	o.m[index] = append(o.m[index], pu)
	o.ml[index].Unlock()
	return pu.slicePool
}

// Get get a object from object pool with T
func Get[T any]() *T {
	var a interface{} = (*T)(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	p := op.get(typPtr)
	v := p.Get()
	if v != nil {
		return v.(*T)
	}
	return new(T)
}

// Put put a object to object pool with T
func Put[T any](t *T) {
	var a interface{} = (*T)(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	p := op.get(typPtr)
	p.Put(t)
}

// Slice  is a slice object pool for T
// []T put sync.Pool is invalid
type Slice[T any] struct {
	Data []T
}

type slicePool struct {
	pools [32]sync.Pool
}

func index(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}

func getSlicePool[T any](s *slicePool, cap int, minCap int) *Slice[T] {
	if cap > math.MaxInt32 {
		return &Slice[T]{Data: make([]T, cap)}
	}

	if cap < minCap { // 小内存分配太零散了。128字节起步，复用率比较高
		cap = minCap
	}

	idx := index(uint32(cap))
	if v := s.pools[idx].Get(); v != nil {
		bp := v.(*Slice[T])
		return bp
	}
	return &Slice[T]{Data: make([]T, 0, 1<<idx)}
}

func putSlicePool[T any](s *slicePool, t *Slice[T]) {
	t.Data = t.Data[:0]
	c := cap(t.Data)
	idx := index(uint32(c))
	if c != 1<<idx { // 不是Get获取的[]byte，放在前一个索引的Pool里面
		idx--
	}
	s.pools[idx].Put(t)
}

// GetSlice get a slice from object pool with T,len() == 0
func GetSlice[T any](cap int) *Slice[T] {
	var a interface{} = (*Slice[T])(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	s := op.getSlice(typPtr)
	var minCap int
	if typPtr != bytesPtr {
		minCap = otherMinCap
	} else {
		minCap = byteMinCap
	}
	return getSlicePool[T](s, cap, minCap)
}

// GetSliceForSize  get a slice from object pool with T and size, len() == size
func GetSliceForSize[T any](size int) *Slice[T] {
	s := GetSlice[T](size)
	s.Data = s.Data[:size]
	return s
}

// PutSlice put a slice to object pool with T
func PutSlice[T any](t *Slice[T]) {
	if cap(t.Data) > math.MaxInt32 {
		return
	}
	var a interface{} = (*Slice[T])(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	if typPtr != bytesPtr {
		if cap(t.Data) < otherMinCap {
			return
		}
	} else {
		if cap(t.Data) < byteMinCap {
			return
		}
	}

	s := op.getSlice(typPtr)
	putSlicePool(s, t)
}

func PutSliceClear[T any](t *Slice[T]) {
	if cap(t.Data) > math.MaxInt32 {
		return
	}
	var a interface{} = (*Slice[T])(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	if typPtr != bytesPtr {
		if cap(t.Data) < otherMinCap {
			return
		}
		clear(t.Data)
	} else {
		if cap(t.Data) < byteMinCap {
			return
		}
	}

	s := op.getSlice(typPtr)
	putSlicePool(s, t)
}

// GetMap  get a map from object pool with K and V
func GetMap[K comparable, V any]() map[K]V {
	var a interface{} = (map[K]V)(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	p := op.get(typPtr)
	v := p.Get()
	if v != nil {
		return v.(map[K]V)
	}
	return map[K]V{}
}

// PutMap put a map to object pool with K and V
func PutMap[K comparable, V any](t map[K]V) {
	clear(t)
	var a interface{} = (map[K]V)(nil)
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	p := op.get(typPtr)
	p.Put(t)
}

type Bytes Slice[byte]
