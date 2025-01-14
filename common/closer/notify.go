package closer

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/ravinggo/game/common/logger"
)

func WaitClose(f func()) {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	sig := <-ch
	logger.Log.Warn().Msg("--------------------------------------------------------------------")
	logger.Log.Warn().Str("signal", sig.String()).Msg("Receive Signal,server is shutting down...")
	logger.Log.Warn().Msg("--------------------------------------------------------------------")
	f()
}
