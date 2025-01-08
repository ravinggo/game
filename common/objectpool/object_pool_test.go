package objectpool

import (
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

func TestGet(t *testing.T) {
	GetPtr[*Struct1]()

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
	m1 := GetMap[string, string]()
	m1["1"] = "1"
	PutMap(m)
	m1 = GetMap[string, string]()
	if len(m) != 0 {
		t.Errorf("GetMap[string, string]() len is not %d", 0)
	}
	ptrCheck := map[uintptr]struct{}{}
	addSet(ptrCheck, GetMapPtr[int, int](), GetMapPtr[string, int]())
	addSet(ptrCheck, GetMapPtr[string, string](), GetMapPtr[int, string](), GetMapPtr[int, Struct1]())
	addSet(ptrCheck, GetMapPtr[Struct1, string](), GetMapPtr[string, Struct1](), GetMapPtr[Struct1, Struct1]())
	addSet(ptrCheck, GetPtr[int](), GetPtr[string](), GetPtr[Struct1](), GetMapPtr[Struct1, *Struct1]())
	addSet(ptrCheck, GetPtr[Struct](), GetPtr[*Struct](), GetPtr[*Struct1](), GetPtr[Slice[int]]())
	addSet(ptrCheck, GetPtr[Slice[int64]](), GetPtr[int64](), GetPtr[*int64](), GetPtr[Slice[byte]]())
	addSet(ptrCheck, GetPtr[Slice[*int64]](), GetPtr[uint64](), GetPtr[*uint64](), GetPtr[Slice[rune]]())

	out := GetSlice[*Struct1](1)
	to := reflect.TypeOf(out.Data)
	if to.Kind() != reflect.Slice {
		t.Errorf("GetSlice[*Struct1](1) is not slice")
	}
	if to.Elem().Kind() != reflect.Ptr {
		t.Errorf("GetSlice[*Struct1](1).Data Type is not ptr")
	}

}

func addSet(m map[uintptr]struct{}, keys ...uintptr) {
	for _, key := range keys {
		if _, ok := m[key]; ok {
			panic("key already exist")
		}
		m[key] = struct{}{}
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
