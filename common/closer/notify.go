package closer

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/ravinggo/game/common/logger"
)

// WaitClose blocks the calling goroutine until SIGINT, SIGHUP, or SIGQUIT is received,
// then logs the shutdown event and calls f to perform graceful cleanup. It is typically
// invoked from main after all services have been started.
// Written by Claude Code claude-opus-4-6.
func WaitClose(f func()) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	sig := <-ch
	logger.Log.Warn().Msg("--------------------------------------------------------------------")
	logger.Log.Warn().Str("signal", sig.String()).Msg("Receive Signal,server is shutting down...")
	logger.Log.Warn().Msg("--------------------------------------------------------------------")
	f()
}
