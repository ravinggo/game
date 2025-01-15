package objectpool

import (
	"math"
	"math/bits"
	"reflect"
	"strconv"
	"sync"
	"unsafe"

	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/utils"
)

const (
	maxIndex    = math.MaxUint16 - 1
	otherMinCap = 16
	byteMinCap  = 128
	KindMask    = (1 << 5) - 1
)

func GetTypePool[T any]() *sync.Pool {
	return get[T](&op, GetPtr[T]())
}

func newPool[T any]() *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return new(T)
		},
	}
}

// Type copy from abi.Type
type Type struct {
	Size_       uintptr
	PtrBytes    uintptr
	Hash        uint32
	TFlag       uint8
	Align_      uint8
	FieldAlign_ uint8
	Kind_       uint8
	Equal       func(unsafe.Pointer, unsafe.Pointer) bool
	GCData      *byte
	Str         int32
	PtrToThis   int32
}

type MapType struct {
	Type
	Key    *Type
	Elem   *Type
	Bucket *Type // internal type representing a hash bucket
	// function for hashing keys (ptr to key, seed) -> hash
	Hasher     func(unsafe.Pointer, uintptr) uintptr
	KeySize    uint8  // size of key slot
	ValueSize  uint8  // size of elem slot
	BucketSize uint16 // size of bucket
	Flags      uint32
}

// PtrType copy from abi.PtrType
type PtrType struct {
	Type
	Elem *Type
}

// GetPtr get type pointer of T
func GetPtr[T any]() uintptr {
	var a any = (*T)(nil)
	t := *(**Type)(unsafe.Pointer(&a))
	if t.Kind_&KindMask == uint8(reflect.Pointer) {
		t = (*PtrType)(unsafe.Pointer(t)).Elem
	}
	return (uintptr)(unsafe.Pointer(t))
}

// GetPtrAny get type pointer of param a
func GetPtrAny(a any) uintptr {
	t := *(**Type)(unsafe.Pointer(&a))
	if t.Kind_&KindMask == uint8(reflect.Pointer) {
		t = (*PtrType)(unsafe.Pointer(t)).Elem
	}
	return (uintptr)(unsafe.Pointer(t))
}

// GetMapPtr get type pointer of map[K]V
func GetMapPtr[K comparable, V any]() uintptr {
	var a any = (map[K]V)(nil)
	t := *(**Type)(unsafe.Pointer(&a))
	return (uintptr)(unsafe.Pointer(t))
}

var (
	op       = objectPool{}
	bytesPtr = func() uintptr {
		return GetPtr[Slice[byte]]()
	}()
	bytesPool = op.getSlice(bytesPtr)
)

type poolUintptr struct {
	uintptr
	singlePool sync.Pool
	slicePool  slicePool
}

type objectPool struct {
	m  [math.MaxUint16][]poolUintptr
	ml [math.MaxUint16]sync.RWMutex
}

func get[T any](o *objectPool, p uintptr) *sync.Pool {
	index := (p >> 6) & maxIndex
	lock := &o.ml[index]
	lock.RLock()
	ss := o.m[index]
	for i := range ss {
		if ss[i].uintptr == p {
			lock.RUnlock()
			return &ss[i].singlePool
		}
	}
	lock.RUnlock()

	// lock for index conflict,
	lock.Lock()
	ss = o.m[index]
	for i := range ss {
		if ss[i].uintptr == p {
			lock.Unlock()
			return &ss[i].singlePool
		}
	}
	ss = append(
		ss, poolUintptr{
			uintptr: p,
			singlePool: sync.Pool{
				New: func() any {
					return new(T)
				},
			},
		},
	)
	o.m[index] = ss
	po := &ss[len(ss)-1]
	lock.Unlock()
	return &po.singlePool
}

func getMap[K comparable, V any](o *objectPool, p uintptr) *sync.Pool {
	index := (p >> 6) & maxIndex
	lock := &o.ml[index]
	lock.RLock()
	ss := o.m[index]
	for i := range ss {
		if ss[i].uintptr == p {
			lock.RUnlock()
			return &ss[i].singlePool
		}
	}
	lock.RUnlock()

	// lock for index conflict,
	lock.Lock()
	ss = o.m[index]
	for i := range ss {
		if ss[i].uintptr == p {
			lock.Unlock()
			return &ss[i].singlePool
		}
	}
	ss = append(
		ss, poolUintptr{
			uintptr: p,
			singlePool: sync.Pool{
				New: func() any {
					return map[K]V{}
				},
			},
		},
	)

	o.m[index] = ss
	po := &ss[len(ss)-1]
	lock.Unlock()
	return &po.singlePool
}

func (o *objectPool) getSlice(p uintptr) *slicePool {
	index := (p >> 6) & maxIndex
	lock := &o.ml[index]
	lock.RLock()
	ss := o.m[index]
	for i := range ss {
		if ss[i].uintptr == p {
			lock.RUnlock()
			return &ss[i].slicePool
		}
	}
	lock.RUnlock()

	// lock for index conflict,
	lock.Lock()
	ss = o.m[index]
	for i := range ss {
		if ss[i].uintptr == p {
			lock.Unlock()
			return &ss[i].slicePool
		}
	}
	ss = append(
		ss, poolUintptr{
			uintptr: p,
		},
	)
	o.m[index] = ss
	po := &ss[len(ss)-1]
	lock.Unlock()
	return &po.slicePool
}

// Get a object from object pool with T
func Get[T any]() *T {
	return get[T](&op, GetPtr[T]()).Get().(*T)
}

// Put a object to object pool with T
func Put[T any](t *T) {
	var a any = t
	if c, ok := a.(define.Clear); ok {
		c.Reset()
	}
	get[T](&op, GetPtr[T]()).Put(t)
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
	typPtr := GetPtr[Slice[T]]()
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
	typPtr := GetPtr[Slice[T]]()
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
	typPtr := GetPtr[Slice[T]]()
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
	return getMap[K, V](&op, GetMapPtr[K, V]()).Get().(map[K]V)
}

// PutMap put a map to object pool with K and V
func PutMap[K comparable, V any](t map[K]V) {
	clear(t)
	getMap[K, V](&op, GetMapPtr[K, V]()).Put(t)
}

// Bytes is a slice object pool for byte
type Bytes Slice[byte]

func GetBytes(cap int) *Bytes {
	b := (*Bytes)(getSlicePool[byte](bytesPool, cap, byteMinCap))
	b.Data = b.Data[:0]
	return b
}

func PutBytes(b *Bytes) {
	PutSlice((*Slice[byte])(b))
}

func (b *Bytes) WriteString(s string) {
	b.Data = append(b.Data, s...)
}

func (b *Bytes) WriteBytes(c ...byte) {
	b.Data = append(b.Data, c...)
}

func (b *Bytes) String() string {
	return utils.BytesToString(b.Data)
}

func (b *Bytes) WriteInt(i int64) {
	b.Data = strconv.AppendInt(b.Data, i, 10)
}

func (b *Bytes) WriteUint(i uint64) {
	b.Data = strconv.AppendUint(b.Data, i, 10)
}

func (b *Bytes) WriteFloat(f float64) {
	b.Data = strconv.AppendFloat(b.Data, f, 'g', -1, 64)
}

func (b *Bytes) WriteBool(v bool) {
	if v {
		b.WriteBytes('T')
	} else {
		b.WriteBytes('F')
	}
}

func (b *Bytes) Len() int {
	return len(b.Data)
}

func (b *Bytes) Cap() int {
	return cap(b.Data)
}

func (b *Bytes) Bytes() []byte {
	return b.Data
}

func (b *Bytes) Reset() {
	b.Data = b.Data[:0]
}
