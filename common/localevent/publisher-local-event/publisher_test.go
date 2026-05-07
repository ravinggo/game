package publisherlocalevent

import (
	"os"
	"testing"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/logger"
)

// TestMain initialises the logger before any test or benchmark runs, preventing
// nil-pointer panics in Logger() and other log-emitting code paths.
// Written by Claude Code claude-opus-4-6.
func TestMain(m *testing.M) {
	logger.InitDefaultLogger()
	Logger()
	os.Exit(m.Run())
}

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

func BenchmarkLocalEvent1(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	call := MiddleLocalEvent[ctx.IntTrace](
		func(ctx *ctx.Int64TraceCtx) *berror.ErrMsg {
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
}
