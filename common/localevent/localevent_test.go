package localevent

import (
	"testing"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/logger"
)

type testStruct struct {
	Id int
}

func (ts testStruct) call(x ctx.IContext, t testStruct) *berror.ErrMsg {
	return nil
}

var c ctx.BaseContext

func BenchmarkLocalEvent(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := Call(nil, testStruct{Id: i})
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkLocalEvent1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	call := MiddleLocalEvent[*ctx.BaseContext](
		func(ctx *ctx.BaseContext) *berror.ErrMsg {
			return nil
		},
	)
	for i := 0; i < b.N; i++ {
		Publish(&c, testStruct{Id: 1})
		err := call(&c)
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
	Logger(logger.Log.Info())
}
