package safego

import (
	"github.com/rs/zerolog"

	"github.com/ravinggo/game/common/logger"
)

func Recover() {
	if e := recover(); e != nil {
		logger.Log.Error().Any("panic info", e).Msg("panic")
	}
}

func RecoverFunc(panicHandler func(e interface{})) {
	if e := recover(); e != nil {
		if panicHandler != nil {
			panicHandler(e)
		}
	}
}

func RecoverWithLogger(log zerolog.Logger) {
	if e := recover(); e != nil {
		log.Error().Any("panic info", e).Msg("panic")
	}
}

func Go(f func()) {
	go func() {
		defer Recover()
		f()
	}()
}

func GOWithLogger(log zerolog.Logger, f func()) {
	go func() {
		defer RecoverWithLogger(log)
		f()
	}()
}

func GOWithPanic(panicFunc func(interface{}), f func()) {
	go func() {
		defer RecoverFunc(panicFunc)
		f()
	}()
}
