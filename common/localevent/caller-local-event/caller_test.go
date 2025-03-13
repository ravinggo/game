package callerlocalevent

import (
	"testing"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
)

type testStruct struct {
	Id int
}

func (ts testStruct) call(x *ctx.Int64TraceCtx, t testStruct) *berror.ErrMsg {
	return nil
}

var c ctx.Int64TraceCtx

func BenchmarkLocalEvent(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := Call(&c, testStruct{Id: i})
		if err != nil {
			panic(err)
		}
	}
}

func init() {
	ts := testStruct{}
	Register(
		"test", ts.call,
	)
	Logger()
}
