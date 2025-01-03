package objectpool

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"unsafe"
)

type Struct struct {
	A unsafe.Pointer
}

type Struct1 struct {
	a int
	b string
}

type Struct2 struct {
	a int
	b string
}

func Get1[T any]() *T {
	var a interface{} = (*T)(nil)
	c := reflect.TypeOf(a)
	fmt.Println(c.String())
	typPtr1 := **(**uintptr)(unsafe.Pointer(&c))
	typPtr := *(*uintptr)(unsafe.Pointer(&a))
	fmt.Println(typPtr1, typPtr)

	var t1 T
	to := reflect.TypeOf(t1)
	fmt.Println(to.String())
	typPtr3 := **(**uintptr)(unsafe.Pointer(&to))
	fmt.Println(typPtr3)
	return new(T)
}

func TestGet(t *testing.T) {
	GetPtr[*Struct1]()
	Get1[Struct1]()

	s1 := Get[Struct1]()
	Put(s1)
	s2 := Get[Struct1]()
	if unsafe.Pointer(s1) != unsafe.Pointer(s2) {
		t.Error("s1 not equal s2")
	}
	Put(s2)

	s3 := Get[Struct2]()
	if unsafe.Pointer(s2) == unsafe.Pointer(s3) {
		t.Error("s2 equal s3")
	}

	bs := GetSlice[byte](0)
	if cap(bs.Data) != byteMinCap {
		t.Errorf("GetSlice[byte](0) cap is not %d", byteMinCap)
	}
	bs = GetSliceForSize[byte](129)
	if cap(bs.Data) != 256 {
		t.Errorf("GetSlice[byte](129) cap is not %d", 256)
	}
	if len(bs.Data) != 129 {
		t.Errorf("GetSlice[byte](129) len is not %d", 129)
	}
	PutSlice(bs)
	bss := GetSlice[Struct1](0)
	if cap(bss.Data) != otherMinCap {
		t.Errorf("GetSlice[Struct1](0)cap is not %d", otherMinCap)
	}
	PutSlice(bss)
	bss = GetSlice[Struct1](0)
	if cap(bss.Data) != otherMinCap {
		t.Errorf("GetSlice[Struct1](0)cap is not %d", otherMinCap)
	}
	PutSlice(bss)
	bss1 := GetSliceForSize[*Struct1](17)
	if cap(bss1.Data) != 32 {
		t.Errorf("GetSlice[Struct1](0)cap is not %d", 32)
	}
	if len(bss1.Data) != 17 {
		t.Errorf("GetSlice[Struct1](17) len is not %d", 17)
	}
	PutSlice(bss1)

	m := GetMap[int, int]()
	m[1] = 1
	PutMap(m)
	m = GetMap[int, int]()
	if len(m) != 0 {
		t.Errorf("GetMap[int, int]() len is not %d", 0)
	}
}

func BenchmarkGetPut(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Put(Get[Struct]())
	}
}

func BenchmarkGetSlicePutSlice(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PutSlice(GetSlice[Struct](0))
	}
}

func BenchmarkGetMapPutMap(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PutMap(GetMap[int, Struct]())
	}
}

var p = sync.Pool{
	New: func() interface{} {
		return new(Struct)
	},
}

func BenchmarkPool(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p.Put(p.Get())
	}
}

func BenchmarkParallel(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(
		func(pb *testing.PB) {
			count := 0
			for pb.Next() {
				count++
				switch count % 4 {
				case 0:
					Put(Get[Struct]())
				case 1:
					PutSlice(GetSlice[byte](0))
				case 2:
					PutSlice(GetSlice[Struct1](128))
				case 3:
					PutMap(GetMap[int, Struct]())
				}
			}
		},
	)
}
