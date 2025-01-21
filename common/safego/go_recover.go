package safego

import (
	"github.com/ravinggo/game/common/logger"
)

// Recover panic of default logger
func Recover() {
	if e := recover(); e != nil {
		logger.Log.Error().Any("panic info", e).Msg("panic")
	}
}

// RecoverFunc panic of panicHandler
func RecoverFunc(panicHandler func(e interface{})) {
	if e := recover(); e != nil {
		if panicHandler != nil {
			panicHandler(e)
		}
	}
}

// RecoverWithLogger panic of log
func RecoverWithLogger(log logger.ILogger) {
	if e := recover(); e != nil {
		log.Error().Any("panic info", e).Msg("panic")
	}
}

// Go run f with Recover
func Go(f func()) {
	go func() {
		defer Recover()
		f()
	}()
}

// GOWithLogger run f with RecoverWithLogger
func GOWithLogger(log logger.ILogger, f func()) {
	go func() {
		defer RecoverWithLogger(log)
		f()
	}()
}

// GOWithPanic run f with RecoverFunc
func GOWithPanic(panicFunc func(any), f func()) {
	go func() {
		defer RecoverFunc(panicFunc)
		f()
	}()
}
