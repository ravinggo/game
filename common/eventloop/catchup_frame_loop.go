package eventloop

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"

	"github.com/ravinggo/game/common/logger"
)

// CatchUpFrameLoop is a single-goroutine fixed-rate frame loop that derives
// the target frame count from elapsed run time: target = (now - epoch) /
// interval. Whenever the number of frames actually run is below target it
// runs one frame immediately — with no sleep in between and no upper bound on
// the backlog — so every logical frame is eventually executed even under
// sustained overload. Only when fully caught up does the loop sleep until the
// next frame is due. delta passed to the frame callback is always the fixed
// interval (a deterministic logical timestep).
//
// Compared with AbsFrameLoop this loop never skips frames; a long stall is
// paid back in full as a catch-up burst. Use it when every logical frame must
// run exactly once (e.g. lockstep simulation), and accept that wall-clock
// latency grows while the backlog drains.
//
// The zero value is not usable; construct one with NewCatchUpFrameLoop.
type CatchUpFrameLoop struct {
	frameEventQueue
	interval     time.Duration
	lockOSThread bool
}

// NewCatchUpFrameLoop allocates a CatchUpFrameLoop running at fps frames per
// second. Panics if fps is not positive. When lockOSThread is true the loop
// goroutine pins itself to its OS thread via runtime.LockOSThread.
func NewCatchUpFrameLoop(fps int, lockOSThread bool) *CatchUpFrameLoop {
	l := &CatchUpFrameLoop{
		interval:     frameInterval(fps),
		lockOSThread: lockOSThread,
	}
	l.initFrameQueue()
	return l
}

// Interval returns the fixed frame duration (1s / fps).
func (l *CatchUpFrameLoop) Interval() time.Duration {
	return l.interval
}

// Start launches the frame-loop goroutine. Each frame first drains all events
// queued via PostEventQueue through handle (func() events are invoked
// directly), then calls frame. Optional endF callbacks run in order after the
// final drain when the loop stops.
func (l *CatchUpFrameLoop) Start(frame FrameFunc, handle func(event any), endF ...func()) {
	go l.run(wrapFrameFunc(frame), wrapFrameHandle(handle), wrapFrameEndFuncs(endF))
}

// run is the loop body. target frames owed = elapsed / interval; while the
// executed count is below target, frames run back-to-back without sleeping.
// Once caught up the loop sleeps until the next frame's absolute due time.
func (l *CatchUpFrameLoop) run(frame FrameFunc, handle func(any), endF []func()) {
	gid := goid.Get()
	defer func() {
		logger.Log.Warn().Int64("gid", gid).Msg("CatchUpFrameLoop closed")
		close(l.over)
	}()
	logger.Log.Warn().Int64("gid", gid).Dur("interval", l.interval).Msg("CatchUpFrameLoop run")
	if l.lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	tm := time.NewTimer(l.interval)
	tm.Stop()
	defer tm.Stop()

	epoch := time.Now()
	var frameCount int64
	for atomic.LoadInt32(&l.stopped) == 0 {
		target := int64(time.Since(epoch) / l.interval)
		if frameCount < target {
			frameCount++
			l.drain(handle)
			frame(frameCount, l.interval)
			continue
		}
		next := epoch.Add(time.Duration(frameCount+1) * l.interval)
		if wait := time.Until(next); wait > 0 {
			tm.Reset(wait)
			select {
			case <-tm.C:
			case <-l.stopCh:
			}
		}
	}

	l.drain(handle)
	for _, v := range endF {
		v()
	}
}
