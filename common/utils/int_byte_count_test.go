package utils

import (
	"hash/crc64"
	"testing"
)

func BenchmarkCountIntByte(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CountIntByte(123123123)
	}
}

func BenchmarkCrc64(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		crc64.Checksum([]byte("12312312"), crc64.MakeTable(crc64.ECMA))
	}
}
