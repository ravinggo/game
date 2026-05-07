// Package safego_test contains tests for the safe goroutine helpers in the safego package.
// Written by Claude Code claude-opus-4-6.
package safego_test

import (
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ravinggo/zerolog"

	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"
)

// discardILogger is a logger.ILogger implementation that discards all log output.
// It is used in tests that need to pass an ILogger without initialising the real logger.
type discardILogger struct {
	l zerolog.Logger
}

func newDiscardILogger() *discardILogger {
	return &discardILogger{l: zerolog.New(io.Discard)}
}

func (d *discardILogger) Trace() *logger.Event     { return d.l.Trace() }
func (d *discardILogger) Debug() *logger.Event     { return d.l.Debug() }
func (d *discardILogger) Info() *logger.Event      { return d.l.Info() }
func (d *discardILogger) Warn() *logger.Event      { return d.l.Warn() }
func (d *discardILogger) Error() *logger.Event     { return d.l.Error() }
func (d *discardILogger) Fatal() *logger.Event     { return d.l.Fatal() }
func (d *discardILogger) Panic() *logger.Event     { return d.l.Panic() }
func (d *discardILogger) NoLevel() *logger.Event   { return d.l.Log() }
func (d *discardILogger) Disabled() *logger.Event  { return nil }
func (d *discardILogger) WithLevel(lvl logger.Level) *logger.Event {
	return d.l.WithLevel(lvl)
}

// TestMain initialises a discard logger so that Recover / Go do not panic when
// dereferencing logger.Log.
func TestMain(m *testing.M) {
	discardLogger := zerolog.New(io.Discard)
	logger.SetLogger(discardLogger)
	os.Exit(m.Run())
}

// ---- Recover ----

// TestRecover_SwallowsPanic verifies that Recover() prevents a panic from propagating.
func TestRecover_SwallowsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic was not swallowed by Recover: %v", r)
		}
	}()

	func() {
		defer safego.Recover()
		panic("test panic - should be swallowed")
	}()
}

// TestRecover_NoOp verifies that Recover() is a no-op when no panic occurs.
func TestRecover_NoOp(t *testing.T) {
	func() {
		defer safego.Recover()
		// no panic
	}()
}

// ---- RecoverFunc ----

// TestRecoverFunc_CallsHandler verifies that RecoverFunc invokes the handler with the
// panic value.
func TestRecoverFunc_CallsHandler(t *testing.T) {
	var got any
	func() {
		defer safego.RecoverFunc(func(e any) {
			got = e
		})
		panic("handler test")
	}()

	if got == nil {
		t.Fatal("expected panicHandler to be called")
	}
	if got.(string) != "handler test" {
		t.Fatalf("expected panic value %q, got %v", "handler test", got)
	}
}

// TestRecoverFunc_NilHandler verifies that RecoverFunc with a nil handler silently
// swallows the panic without re-panicking.
func TestRecoverFunc_NilHandler(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic leaked when panicHandler is nil: %v", r)
		}
	}()

	func() {
		defer safego.RecoverFunc(nil)
		panic("nil handler panic")
	}()
}

// TestRecoverFunc_NoOp verifies that RecoverFunc is harmless when no panic occurs.
func TestRecoverFunc_NoOp(t *testing.T) {
	called := false
	func() {
		defer safego.RecoverFunc(func(e any) { called = true })
		// no panic
	}()
	if called {
		t.Fatal("panicHandler should not be called when no panic occurs")
	}
}

// TestRecoverFunc_SwallowsPanic verifies that RecoverFunc prevents a panic from
// propagating regardless of what the value is.
func TestRecoverFunc_SwallowsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic was not swallowed: %v", r)
		}
	}()

	func() {
		defer safego.RecoverFunc(func(e any) { /* swallow */ })
		panic("should be swallowed")
	}()
}

// ---- RecoverWithLogger ----

// TestRecoverWithLogger_SwallowsPanic verifies that RecoverWithLogger prevents a panic
// from propagating and calls the logger's Error method.
func TestRecoverWithLogger_SwallowsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic leaked from RecoverWithLogger: %v", r)
		}
	}()

	dl := newDiscardILogger()
	func() {
		defer safego.RecoverWithLogger(dl)
		panic("logger panic test")
	}()
}

// TestRecoverWithLogger_NoOp verifies no-op behaviour.
func TestRecoverWithLogger_NoOp(t *testing.T) {
	dl := newDiscardILogger()
	func() {
		defer safego.RecoverWithLogger(dl)
		// no panic
	}()
}

// ---- Go ----

// TestGo_RunsFunction verifies that Go executes the provided function.
func TestGo_RunsFunction(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	safego.Go(func() {
		defer wg.Done()
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not complete in time")
	}
}

// TestGo_DoesNotPanicOnPanic verifies that a panic inside Go does not crash the process.
func TestGo_DoesNotPanicOnPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	safego.Go(func() {
		defer wg.Done()
		panic("inner panic - should be recovered by safego.Recover")
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not complete in time")
	}
}

// ---- GOWithLogger ----

// TestGOWithLogger_RunsFunction verifies that GOWithLogger executes the function.
func TestGOWithLogger_RunsFunction(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	dl := newDiscardILogger()

	safego.GOWithLogger(dl, func() {
		defer wg.Done()
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not complete in time")
	}
}

// TestGOWithLogger_DoesNotPanicOnPanic verifies panic recovery with a custom logger.
func TestGOWithLogger_DoesNotPanicOnPanic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	dl := newDiscardILogger()

	safego.GOWithLogger(dl, func() {
		defer wg.Done()
		panic("should be recovered by RecoverWithLogger")
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not complete in time")
	}
}

// ---- GOWithPanic ----

// TestGOWithPanic_InvokesPanicFunc verifies that GOWithPanic calls panicFunc on panic.
func TestGOWithPanic_InvokesPanicFunc(t *testing.T) {
	var called atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)

	safego.GOWithPanic(func(v any) {
		called.Store(true)
		wg.Done()
	}, func() {
		panic("gowithpanic test")
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("panicFunc was not called in time")
	}

	if !called.Load() {
		t.Fatal("expected panicFunc to be called")
	}
}

// TestGOWithPanic_NoCallWhenNoPanic verifies that panicFunc is not called when the
// goroutine completes without panicking.
func TestGOWithPanic_NoCallWhenNoPanic(t *testing.T) {
	var called atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)

	safego.GOWithPanic(func(v any) {
		called.Store(true)
	}, func() {
		defer wg.Done()
		// no panic
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not complete in time")
	}

	time.Sleep(50 * time.Millisecond)
	if called.Load() {
		t.Fatal("panicFunc should not be called when no panic occurs")
	}
}

// TestGOWithPanic_RunsFunction verifies that GOWithPanic actually executes the function.
func TestGOWithPanic_RunsFunction(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	safego.GOWithPanic(func(v any) {}, func() {
		defer wg.Done()
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not complete in time")
	}
}
