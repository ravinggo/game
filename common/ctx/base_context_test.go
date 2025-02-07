package ctx

import (
	"testing"
)

type (
	objectPoolKey struct{}
)

func BenchmarkGetValue(b *testing.B) {
	var c Int64TraceCtx
	c.SetValue(objectPoolKey{}, 1)
	for i := 0; i < b.N; i++ {
		Value[objectPoolKey, int](&c, objectPoolKey{})
	}
}
