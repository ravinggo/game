package natsclient

import (
	"hash/crc32"
	"testing"
)

func BenchmarkGetNatsClientWith(b *testing.B) {
	x := []byte("12345678")
	for i := 0; i < b.N; i++ {
		crc32.ChecksumIEEE(x)
	}
}
