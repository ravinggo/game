package eventloop

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"
)

// FrameFunc is the per-frame callback of a frame loop. frameCount starts at 1
// and increases monotonically; delta is the logical timestep for AbsFrameLoop
// (always the fixed frame interval) and the actual elapsed time since the
// previous frame for TickerFrameLoop.
type FrameFunc func(frameCount int64, delta time.Duration)

// frameEventQueue is the shared event buffer of the frame loops. Producers
// append into pending under a short mutex; the loop goroutine swaps the slice
// out at each frame boundary and processes it without holding the lock.
type frameEventQueue struct {
	mu      sync.Mutex
	pending []any
	scratch []any
	stopped int32
	stopCh  chan struct{}
	over    chan struct{}
}

func (q *frameEventQueue) initFrameQueue() {
	q.stopCh = make(chan struct{})
	q.over = make(chan struct{})
}

// PostEventQueue enqueues event e for processing at the next frame boundary.
// If the loop has already been stopped the call is a no-op.
// Safe for concurrent use by multiple goroutines.
func (q *frameEventQueue) PostEventQueue(e any) {
	if atomic.LoadInt32(&q.stopped) != 0 {
		return
	}
	q.mu.Lock()
	q.pending = append(q.pending, e)
	q.mu.Unlock()
}

// drain swaps out the pending buffer and invokes f for every queued event.
// Only the loop goroutine may call drain.
func (q *frameEventQueue) drain(f func(any)) {
	q.mu.Lock()
	evs := q.pending
	q.pending = q.scratch
	q.mu.Unlock()
	for _, e := range evs {
		f(e)
	}
	clear(evs)
	q.scratch = evs[:0]
}

// Stop signals the loop to stop and blocks until it has drained remaining
// events and exited. Safe to call more than once.
func (q *frameEventQueue) Stop() {
	if atomic.CompareAndSwapInt32(&q.stopped, 0, 1) {
		close(q.stopCh)
	}
	<-q.over
}

// Stopped reports whether the loop has been stopped.
func (q *frameEventQueue) Stopped() bool {
	return atomic.LoadInt32(&q.stopped) == 1
}

// wrapFrameHandle guards the event handler with a recover and, as with
// DoubleBuffQueue, calls func() events directly instead of passing them to f.
func wrapFrameHandle(f func(any)) func(any) {
	return func(e any) {
		defer safego.Recover()
		switch fx := e.(type) {
		case func():
			fx()
		default:
			f(e)
		}
	}
}

// wrapFrameFunc guards the per-frame callback with a recover so a panic in one
// frame does not kill the loop goroutine. A nil frame becomes a no-op.
func wrapFrameFunc(frame FrameFunc) FrameFunc {
	if frame == nil {
		return func(int64, time.Duration) {}
	}
	return func(frameCount int64, delta time.Duration) {
		defer safego.Recover()
		frame(frameCount, delta)
	}
}

// wrapFrameEndFuncs guards each endF with a recover so a panic in one does not
// prevent the remaining callbacks from running.
func wrapFrameEndFuncs(endF []func()) []func() {
	rF := make([]func(), 0, len(endF))
	for _, v := range endF {
		v := v
		rF = append(
			rF, func() {
				defer safego.RecoverFunc(
					func(e any) {
						logger.Log.Error().Any("panic info", e).Msg("frame loop end func panic")
					},
				)
				v()
			},
		)
	}
	return rF
}

// frameInterval converts frames-per-second into a frame duration, panicking on
// non-positive fps because a frame loop cannot run without a valid rate.
func frameInterval(fps int) time.Duration {
	if fps <= 0 {
		panic("frame loop: fps must be positive")
	}
	return time.Second / time.Duration(fps)
}
