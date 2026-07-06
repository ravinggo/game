// Tests for AbsFrameLoop and TickerFrameLoop — pacing, event delivery, catch-up.
package eventloop

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAbsFrameLoop_RunsAtConfiguredRate verifies that the absolute-time loop
// delivers roughly fps frames per second (generous bounds for CI timer jitter;
// the lower bound holds because late frames are caught up).
func TestAbsFrameLoop_RunsAtConfiguredRate(t *testing.T) {
	l := NewAbsFrameLoop(50, false)
	var frames atomic.Int64
	l.Start(
		func(frameCount int64, delta time.Duration) {
			if delta != l.Interval() {
				t.Errorf("abs loop delta = %v, want fixed interval %v", delta, l.Interval())
			}
			frames.Store(frameCount)
		}, func(any) {},
	)
	time.Sleep(500 * time.Millisecond)
	l.Stop()

	got := frames.Load()
	if got < 18 || got > 35 {
		t.Fatalf("frames after 500ms at 50fps = %d, want ~25 (18..35)", got)
	}
}

// TestAbsFrameLoop_CatchesUpAfterStall verifies the defining property of
// absolute pacing: a long frame does not permanently lower the frame count —
// the loop runs back-to-back frames until it is back on schedule.
func TestAbsFrameLoop_CatchesUpAfterStall(t *testing.T) {
	l := NewAbsFrameLoop(100, false)
	l.SetMaxCatchUpFrames(1000) // force catch-up, never skip
	var frames atomic.Int64
	var stalled atomic.Bool
	l.Start(
		func(frameCount int64, _ time.Duration) {
			if stalled.CompareAndSwap(false, true) {
				time.Sleep(200 * time.Millisecond)
			}
			frames.Store(frameCount)
		}, func(any) {},
	)
	time.Sleep(500 * time.Millisecond)
	l.Stop()

	got := frames.Load()
	if got < 38 {
		t.Fatalf("frames after 500ms at 100fps with 200ms stall = %d, want >=38 (catch-up)", got)
	}
}

// TestAbsFrameLoop_SkipsFramesBeyondMaxCatchUp verifies that when the backlog
// exceeds maxCatchUp frames the loop jumps frameCount forward instead of
// running an unbounded catch-up burst.
func TestAbsFrameLoop_SkipsFramesBeyondMaxCatchUp(t *testing.T) {
	l := NewAbsFrameLoop(100, false)
	l.SetMaxCatchUpFrames(2)
	var mu sync.Mutex
	var counts []int64
	var stalled atomic.Bool
	l.Start(
		func(frameCount int64, _ time.Duration) {
			if stalled.CompareAndSwap(false, true) {
				time.Sleep(150 * time.Millisecond)
			}
			mu.Lock()
			counts = append(counts, frameCount)
			mu.Unlock()
		}, func(any) {},
	)
	time.Sleep(400 * time.Millisecond)
	l.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(counts) < 2 {
		t.Fatalf("too few frames ran: %d", len(counts))
	}
	maxGap := int64(0)
	for i := 1; i < len(counts); i++ {
		if gap := counts[i] - counts[i-1]; gap > maxGap {
			maxGap = gap
		}
	}
	if maxGap <= 1 {
		t.Fatalf("expected a frameCount jump after 150ms stall with maxCatchUp=2, got contiguous counts")
	}
}

// TestAbsFrameLoop_EventsDrainBeforeFrame verifies that events queued before a
// frame boundary are handled before that frame's callback runs.
func TestAbsFrameLoop_EventsDrainBeforeFrame(t *testing.T) {
	l := NewAbsFrameLoop(100, false)
	var mu sync.Mutex
	var order []string
	var once sync.Once
	done := make(chan struct{})
	l.PostEventQueue("evt")
	l.Start(
		func(int64, time.Duration) {
			mu.Lock()
			order = append(order, "frame")
			mu.Unlock()
			once.Do(func() { close(done) })
		}, func(e any) {
			mu.Lock()
			order = append(order, e.(string))
			mu.Unlock()
		},
	)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("first frame did not run within timeout")
	}
	l.Stop()

	mu.Lock()
	defer mu.Unlock()
	if order[0] != "evt" {
		t.Fatalf("first processed item = %q, want the queued event before the first frame", order[0])
	}
}

// TestAbsFrameLoop_FuncEventCalledDirectly verifies the func() shortcut.
func TestAbsFrameLoop_FuncEventCalledDirectly(t *testing.T) {
	l := NewAbsFrameLoop(100, false)
	ran := make(chan struct{})
	l.Start(nil, func(any) { t.Error("func() must not reach the handle callback") })
	l.PostEventQueue(func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("func() event did not run within timeout")
	}
	l.Stop()
}

// TestAbsFrameLoop_StopDrainsEvents verifies that Stop processes events still
// queued at shutdown, and that Stop is idempotent.
func TestAbsFrameLoop_StopDrainsEvents(t *testing.T) {
	l := NewAbsFrameLoop(1, false) // 1fps: events below are queued, not yet drained
	var count atomic.Int64
	l.Start(nil, func(any) { count.Add(1) })
	const n = 10
	for i := 0; i < n; i++ {
		l.PostEventQueue(i)
	}
	l.Stop()
	l.Stop() // must not panic or deadlock

	if got := count.Load(); got != n {
		t.Fatalf("drained %d events on Stop, want %d", got, n)
	}
	if !l.Stopped() {
		t.Fatal("Stopped() should report true after Stop")
	}
}

// TestAbsFrameLoop_ConcurrentPost verifies lossless delivery under concurrent
// producers (run with -race).
func TestAbsFrameLoop_ConcurrentPost(t *testing.T) {
	const goroutines = 16
	const perG = 100
	l := NewAbsFrameLoop(200, false)
	var count atomic.Int64
	l.Start(nil, func(any) { count.Add(1) })

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				l.PostEventQueue(i)
			}
		}()
	}
	wg.Wait()
	l.Stop()

	if got := count.Load(); got != goroutines*perG {
		t.Fatalf("delivered %d events, want %d", got, goroutines*perG)
	}
}

// TestTickerFrameLoop_RunsAtConfiguredRate verifies the ticker loop ticks at
// roughly fps (upper bound strict: a ticker never runs catch-up frames).
func TestTickerFrameLoop_RunsAtConfiguredRate(t *testing.T) {
	l := NewTickerFrameLoop(50, false)
	var frames atomic.Int64
	l.Start(
		func(frameCount int64, delta time.Duration) {
			if delta <= 0 {
				t.Errorf("ticker loop delta = %v, want > 0", delta)
			}
			frames.Store(frameCount)
		}, func(any) {},
	)
	time.Sleep(500 * time.Millisecond)
	l.Stop()

	got := frames.Load()
	if got < 15 || got > 30 {
		t.Fatalf("frames after 500ms at 50fps = %d, want ~25 (15..30)", got)
	}
}

// TestTickerFrameLoop_EventsDelivered verifies queued events are handled and
// Stop drains the remainder.
func TestTickerFrameLoop_EventsDelivered(t *testing.T) {
	l := NewTickerFrameLoop(1, false) // 1fps: events drain on Stop, not on a tick
	var count atomic.Int64
	l.Start(nil, func(any) { count.Add(1) })
	const n = 5
	for i := 0; i < n; i++ {
		l.PostEventQueue(i)
	}
	l.Stop()

	if got := count.Load(); got != n {
		t.Fatalf("delivered %d events, want %d", got, n)
	}
	if !l.Stopped() {
		t.Fatal("Stopped() should report true after Stop")
	}
}

// TestTickerFrameLoop_ConcurrentPost verifies lossless delivery under
// concurrent producers (run with -race).
func TestTickerFrameLoop_ConcurrentPost(t *testing.T) {
	const goroutines = 16
	const perG = 100
	l := NewTickerFrameLoop(200, false)
	var count atomic.Int64
	l.Start(nil, func(any) { count.Add(1) })

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				l.PostEventQueue(i)
			}
		}()
	}
	wg.Wait()
	l.Stop()

	if got := count.Load(); got != goroutines*perG {
		t.Fatalf("delivered %d events, want %d", got, goroutines*perG)
	}
}

// TestNewFrameLoop_InvalidFPSPanics verifies constructors reject fps <= 0.
func TestNewFrameLoop_InvalidFPSPanics(t *testing.T) {
	for _, f := range []func(){
		func() { NewAbsFrameLoop(0, false) },
		func() { NewTickerFrameLoop(-1, false) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("expected panic for non-positive fps")
				}
			}()
			f()
		}()
	}
}
