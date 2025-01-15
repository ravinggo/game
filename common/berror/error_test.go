package berror

import (
	"testing"

	baseenv "github.com/ravinggo/game/common/base-env"
)

func TestStackTrace(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = true
	e := NewDatabaseStr("xxxx")
	if len(e.StackStace) == 0 {
		t.Error("StackTrace not set")
	}
	t.Log(e.String())
}

func BenchmarkErrorString(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	err := NewProtocolStr("xxxxx")
	for i := 0; i < b.N; i++ {
		err.String()
	}
}
