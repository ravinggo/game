// Tests for CatchUpFrameLoop — elapsed-time pacing, no-skip catch-up, delivery.
package eventloop

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCatchUpFrameLoop_RunsAtConfiguredRate verifies the loop tracks
// elapsed/interval (generous bounds for CI timer jitter; the lower bound holds
// because owed frames are always caught up).
func TestCatchUpFrameLoop_RunsAtConfiguredRate(t *testing.T) {
	l := NewCatchUpFrameLoop(50, false)
	var frames atomic.Int64
	l.Start(
		func(frameCount int64, delta time.Duration) {
			if delta != l.Interval() {
				t.Errorf("catch-up loop delta = %v, want fixed interval %v", delta, l.Interval())
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

// TestCatchUpFrameLoop_NeverSkipsFrames verifies the defining property: after
// a long stall every owed frame still runs, with strictly contiguous frame
// counts — no gaps regardless of how far behind the loop fell.
func TestCatchUpFrameLoop_NeverSkipsFrames(t *testing.T) {
	l := NewCatchUpFrameLoop(100, false)
	var mu sync.Mutex
	var counts []int64
	var stalled atomic.Bool
	l.Start(
		func(frameCount int64, _ time.Duration) {
			if stalled.CompareAndSwap(false, true) {
				time.Sleep(200 * time.Millisecond) // owes ~20 frames at 100fps
			}
			mu.Lock()
			counts = append(counts, frameCount)
			mu.Unlock()
		}, func(any) {},
	)
	time.Sleep(500 * time.Millisecond)
	l.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(counts) < 38 {
		t.Fatalf("only %d frames ran after 500ms at 100fps with 200ms stall, want >=38 (full catch-up)", len(counts))
	}
	for i, v := range counts {
		if v != int64(i+1) {
			t.Fatalf("frame counts must be contiguous: index %d has frameCount %d, want %d", i, v, i+1)
		}
	}
}

// TestCatchUpFrameLoop_EventsDrainBeforeFrame verifies events queued before a
// frame boundary are handled before that frame's callback runs.
func TestCatchUpFrameLoop_EventsDrainBeforeFrame(t *testing.T) {
	l := NewCatchUpFrameLoop(100, false)
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

// TestCatchUpFrameLoop_StopDrainsEvents verifies queued events drain on Stop
// and that Stop is idempotent.
func TestCatchUpFrameLoop_StopDrainsEvents(t *testing.T) {
	l := NewCatchUpFrameLoop(1, false) // 1fps: events below drain on Stop
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

// TestCatchUpFrameLoop_ConcurrentPost verifies lossless delivery under
// concurrent producers (run with -race).
func TestCatchUpFrameLoop_ConcurrentPost(t *testing.T) {
	const goroutines = 16
	const perG = 100
	l := NewCatchUpFrameLoop(200, false)
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

// TestNewCatchUpFrameLoop_InvalidFPSPanics verifies the constructor rejects
// fps <= 0.
func TestNewCatchUpFrameLoop_InvalidFPSPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic for non-positive fps")
		}
	}()
	NewCatchUpFrameLoop(0, false)
}
