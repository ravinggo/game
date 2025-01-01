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
