package logger

import (
	"io"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	Log.Info().Str("key", "value").Str("a", "a").Msg("testxxxx")
	Log.Debug().Str("key", "value").Str("a", "a").Msg("testxxxx")
	Log.Warn().Str("key", "value").Str("a", "a").Msg("testxxxx")
	Log.Error().Stack().Str("key", "value").Str("a", "a").Err(os.ErrClosed).Msg("testxxxx")

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
