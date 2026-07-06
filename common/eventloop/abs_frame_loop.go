package eventloop

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"

	"github.com/ravinggo/game/common/logger"
)

// defaultMaxCatchUpFrames bounds how many frames AbsFrameLoop will run
// back-to-back to catch up before it gives up and skips the lost frames,
// preventing a spiral of death when frames persistently overrun the interval.
const defaultMaxCatchUpFrames = 5

// AbsFrameLoop is a single-goroutine fixed-rate frame loop that schedules
// frames against absolute time: frame N is due at epoch + N*interval, where
// epoch is the moment the loop started. Sleep jitter and frame cost do not
// accumulate — if a frame finishes late the loop runs subsequent frames
// without sleeping until it is back on schedule, so the long-run average rate
// is exactly fps. delta passed to the frame callback is always the fixed
// interval (a deterministic logical timestep).
//
// The zero value is not usable; construct one with NewAbsFrameLoop.
type AbsFrameLoop struct {
	frameEventQueue
	interval     time.Duration
	maxCatchUp   int64
	lockOSThread bool
}

// NewAbsFrameLoop allocates an AbsFrameLoop running at fps frames per second.
// Panics if fps is not positive. When lockOSThread is true the loop goroutine
// pins itself to its OS thread via runtime.LockOSThread.
func NewAbsFrameLoop(fps int, lockOSThread bool) *AbsFrameLoop {
	l := &AbsFrameLoop{
		interval:     frameInterval(fps),
		maxCatchUp:   defaultMaxCatchUpFrames,
		lockOSThread: lockOSThread,
	}
	l.initFrameQueue()
	return l
}

// Interval returns the fixed frame duration (1s / fps).
func (l *AbsFrameLoop) Interval() time.Duration {
	return l.interval
}

// SetMaxCatchUpFrames overrides how far behind schedule the loop may fall
// before it skips the lost frames instead of catching up one by one.
// Must be called before Start; n <= 0 is ignored.
func (l *AbsFrameLoop) SetMaxCatchUpFrames(n int64) {
	if n > 0 {
		l.maxCatchUp = n
	}
}

// Start launches the frame-loop goroutine. Each frame first drains all events
// queued via PostEventQueue through handle (func() events are invoked
// directly), then calls frame. Optional endF callbacks run in order after the
// final drain when the loop stops.
func (l *AbsFrameLoop) Start(frame FrameFunc, handle func(event any), endF ...func()) {
	go l.run(wrapFrameFunc(frame), wrapFrameHandle(handle), wrapFrameEndFuncs(endF))
}

// run is the loop body. Each iteration computes the absolute due time of the
// next frame from the start epoch; it sleeps only when ahead of schedule and
// runs immediately when behind. When the backlog exceeds maxCatchUp frames the
// lost frames are skipped (frameCount jumps forward) to avoid an unbounded
// catch-up burst.
func (l *AbsFrameLoop) run(frame FrameFunc, handle func(any), endF []func()) {
	gid := goid.Get()
	defer func() {
		logger.Log.Warn().Int64("gid", gid).Msg("AbsFrameLoop closed")
		close(l.over)
	}()
	logger.Log.Warn().Int64("gid", gid).Dur("interval", l.interval).Msg("AbsFrameLoop run")
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
		next := epoch.Add(time.Duration(frameCount+1) * l.interval)
		now := time.Now()
		if wait := next.Sub(now); wait > 0 {
			tm.Reset(wait)
			select {
			case <-tm.C:
			case <-l.stopCh:
				continue
			}
		} else if behind := now.Sub(next); behind > time.Duration(l.maxCatchUp)*l.interval {
			skipped := int64(behind / l.interval)
			frameCount += skipped
			logger.Log.Warn().
				Int64("skippedFrames", skipped).
				Dur("behind", behind).
				Msg("AbsFrameLoop too far behind schedule, skipping frames")
		}
		frameCount++
		l.drain(handle)
		frame(frameCount, l.interval)
	}

	l.drain(handle)
	for _, v := range endF {
		v()
	}
}
