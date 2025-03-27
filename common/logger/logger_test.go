package logger

import (
	"io"
	"testing"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
)

func init() {
	InitDefaultLogger()
}

func TestLog(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = true
	Log.Info().Str("key", "value").Str("a", "a").Msg("testxxxx")
	Log.Debug().Str("key", "value").Str("a", "a").Msg("testxxxx")
	Log.Warn().Str("key", "value").Str("a", "a").Msg("testxxxx")
	Log.Error().Str("key", "value").Str("a", "a").Err(berror.NewDatabaseStr("123")).Msg("testxxxx")

}

func BenchmarkLogger(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Log.Info().Str("key", "value").Str("a", "a").Str("a", "a").Msg("testxxxx")
	}
}

func TestNewAsync(t *testing.T) {
	w := NewAsync(io.Discard)
	for i := 0; i < 1000000; i++ {
		w.Write([]byte("helloworld"))
	}

	w.Close()

	if count != 1000000*10 {
		t.Fatalf("")
	}
}
