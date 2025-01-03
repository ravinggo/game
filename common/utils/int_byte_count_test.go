package utils

import (
	"testing"
)

func BenchmarkCountIntByte(b *testing.B) {
	for i := 0; i < b.N; i++ {
		CountIntByte(123123123)
	}
}
