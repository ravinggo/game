package eventloop

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"

	"github.com/ravinggo/game/common/logger"
)

// TickerFrameLoop is a single-goroutine fixed-rate frame loop driven by a
// time.Ticker — the most common game-loop implementation. Each tick runs one
// frame; when a frame overruns the interval the ticker drops the missed ticks,
// so the loop never runs catch-up frames and the effective rate simply slips
// below fps under sustained load. delta passed to the frame callback is the
// actual elapsed time since the previous frame.
//
// The zero value is not usable; construct one with NewTickerFrameLoop.
type TickerFrameLoop struct {
	frameEventQueue
	interval     time.Duration
	lockOSThread bool
}

// NewTickerFrameLoop allocates a TickerFrameLoop running at fps frames per
// second. Panics if fps is not positive. When lockOSThread is true the loop
// goroutine pins itself to its OS thread via runtime.LockOSThread.
func NewTickerFrameLoop(fps int, lockOSThread bool) *TickerFrameLoop {
	l := &TickerFrameLoop{
		interval:     frameInterval(fps),
		lockOSThread: lockOSThread,
	}
	l.initFrameQueue()
	return l
}

// Interval returns the fixed frame duration (1s / fps).
func (l *TickerFrameLoop) Interval() time.Duration {
	return l.interval
}

// Start launches the frame-loop goroutine. Each tick first drains all events
// queued via PostEventQueue through handle (func() events are invoked
// directly), then calls frame. Optional endF callbacks run in order after the
// final drain when the loop stops.
func (l *TickerFrameLoop) Start(frame FrameFunc, handle func(event any), endF ...func()) {
	go l.run(wrapFrameFunc(frame), wrapFrameHandle(handle), wrapFrameEndFuncs(endF))
}

// run is the loop body: block on the ticker, run one frame per tick, exit on
// stop and drain whatever is still queued.
func (l *TickerFrameLoop) run(frame FrameFunc, handle func(any), endF []func()) {
	gid := goid.Get()
	defer func() {
		logger.Log.Warn().Int64("gid", gid).Msg("TickerFrameLoop closed")
		close(l.over)
	}()
	logger.Log.Warn().Int64("gid", gid).Dur("interval", l.interval).Msg("TickerFrameLoop run")
	if l.lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	last := time.Now()
	var frameCount int64
	for atomic.LoadInt32(&l.stopped) == 0 {
		select {
		case now := <-ticker.C:
			frameCount++
			l.drain(handle)
			frame(frameCount, now.Sub(last))
			last = now
		case <-l.stopCh:
		}
	}

	l.drain(handle)
	for _, v := range endF {
		v()
	}
}
