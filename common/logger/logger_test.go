package logger

import (
	"testing"
)

func TestLog(t *testing.T) {
	Log.Info().Str("key", "value").Msg("test")
}
