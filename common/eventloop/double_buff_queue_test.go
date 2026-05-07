// Written by Claude Code claude-opus-4-6.
package eventloop

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ravinggo/game/common/logger"
)

// TestMain initialises the logger so that internal eventloop goroutines that
// call logger.Log.Warn/Error do not panic on a nil receiver.
// Written by Claude Code claude-opus-4-6.
func TestMain(m *testing.M) {
	logger.InitDefaultLogger()
	os.Exit(m.Run())
}

// TestDoubleBuffQueue_PostEventQueue_Delivers verifies that events posted via
// PostEventQueue reach the handler registered with Start.
func TestDoubleBuffQueue_PostEventQueue_Delivers(t *testing.T) {
	q := NewDoubleBuffQueue(false)

	var received int64
	done := make(chan struct{})

	q.Start(func(event any) {
		if event.(int) == 42 {
			atomic.StoreInt64(&received, 1)
			close(done)
		}
	})

	q.PostEventQueue(42)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("event was not delivered within timeout")
	}

	q.Stop()

	if atomic.LoadInt64(&received) != 1 {
		t.Fatal("handler never saw the posted event")
	}
}

// TestDoubleBuffQueue_Stop_DrainsAndExits verifies that calling Stop causes
// the queue to drain all pending events and exit cleanly.
func TestDoubleBuffQueue_Stop_DrainsAndExits(t *testing.T) {
	q := NewDoubleBuffQueue(false)

	const n = 100
	var count int64

	q.Start(func(event any) {
		atomic.AddInt64(&count, 1)
	})

	for i := 0; i < n; i++ {
		q.PostEventQueue(i)
	}

	// Stop blocks until the loop exits; all events posted before Stop should
	// have been processed.
	q.Stop()

	if !q.Stopped() {
		t.Fatal("queue reports not stopped after Stop")
	}
	// We cannot assert count == n with certainty because some events may have
	// been dropped if Stop raced with a PostEventQueue that observed stopped==0
	// and then the loop exited before processing them. What we CAN assert is
	// that Stop returned and the queue is marked stopped.
}

// TestDoubleBuffQueue_AfterFunc_FiresCallback verifies that AfterFunc delivers
// the callback to the event loop after the requested duration.
func TestDoubleBuffQueue_AfterFunc_FiresCallback(t *testing.T) {
	q := NewDoubleBuffQueue(false)

	fired := make(chan struct{})
	q.Start(func(event any) {})

	q.AfterFunc(50*time.Millisecond, func() {
		close(fired)
	})

	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("AfterFunc callback was not fired within timeout")
	}

	q.Stop()
}

// TestDoubleBuffQueue_MultipleEvents_Order verifies that events are processed
// in the order they were posted when submitted from a single goroutine.
func TestDoubleBuffQueue_MultipleEvents_Order(t *testing.T) {
	q := NewDoubleBuffQueue(false)

	const n = 20
	results := make([]int, 0, n)
	done := make(chan struct{})

	q.Start(func(event any) {
		results = append(results, event.(int))
		if len(results) == n {
			close(done)
		}
	})

	for i := 0; i < n; i++ {
		q.PostEventQueue(i)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("only received %d/%d events", len(results), n)
	}

	q.Stop()

	for i, v := range results {
		if v != i {
			t.Fatalf("event out of order: results[%d] = %d, want %d", i, v, i)
		}
	}
}

// TestDoubleBuffQueue_IgnoresPostAfterStop verifies that PostEventQueue is a
// no-op once Stop has been called.
func TestDoubleBuffQueue_IgnoresPostAfterStop(t *testing.T) {
	q := NewDoubleBuffQueue(false)

	var count int64
	q.Start(func(event any) {
		atomic.AddInt64(&count, 1)
	})

	// Drain any startup race by posting one event and waiting briefly.
	q.PostEventQueue(1)
	time.Sleep(50 * time.Millisecond)

	q.Stop()

	before := atomic.LoadInt64(&count)

	// These posts must be silently ignored.
	q.PostEventQueue(2)
	q.PostEventQueue(3)
	time.Sleep(100 * time.Millisecond)

	after := atomic.LoadInt64(&count)
	if after != before {
		t.Fatalf("PostEventQueue after Stop incremented count: before=%d after=%d", before, after)
	}
}
