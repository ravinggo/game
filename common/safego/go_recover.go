package safego

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/ravinggo/game/common/logger"
)

// Recover catches any panic from the current goroutine and logs it at ERROR level using
// the global logger. It is intended to be called with defer at the top of a goroutine.
// Written by Claude Code claude-opus-4-6.
func Recover() {
	if e := recover(); e != nil {
		logger.Log.Error().Err(errors.New(fmt.Sprintf("panic:%v", e))).Send()
	}
}

// RecoverFunc catches any panic from the current goroutine and delegates it to
// panicHandler. If panicHandler is nil the panic is silently swallowed.
// Use defer RecoverFunc(fn) at the top of a goroutine.
// Written by Claude Code claude-opus-4-6.
func RecoverFunc(panicHandler func(e interface{})) {
	if e := recover(); e != nil {
		if panicHandler != nil {
			panicHandler(e)
		}
	}
}

// RecoverWithLogger catches any panic from the current goroutine and logs it at ERROR
// level using the provided logger. Use defer RecoverWithLogger(log) at the top of a
// goroutine when a service-specific logger is available.
// Written by Claude Code claude-opus-4-6.
func RecoverWithLogger(log logger.ILogger) {
	if e := recover(); e != nil {
		log.Error().Err(errors.New(fmt.Sprintf("panic:%v", e))).Send()
	}
}

// Go spawns f in a new goroutine protected by Recover so that any panic inside f is
// caught and logged without crashing the process.
// Written by Claude Code claude-opus-4-6.
func Go(f func()) {
	go func() {
		defer Recover()
		f()
	}()
}

// GOWithLogger spawns f in a new goroutine protected by RecoverWithLogger so that panics
// are caught and reported through the provided logger.
// Written by Claude Code claude-opus-4-6.
func GOWithLogger(log logger.ILogger, f func()) {
	go func() {
		defer RecoverWithLogger(log)
		f()
	}()
}

// GOWithPanic spawns f in a new goroutine. If f panics, panicFunc is called with the
// recovered value so the caller can apply custom error handling (e.g. alerting).
// Written by Claude Code claude-opus-4-6.
func GOWithPanic(panicFunc func(any), f func()) {
	go func() {
		defer RecoverFunc(panicFunc)
		f()
	}()
}
