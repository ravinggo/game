package timer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravinggo/game/common/safego"
)

// tickerElemPool recycles tickerElem instances to avoid per-timer heap
// allocations on the hot path.
// Written by Claude Code claude-opus-4-6.
var tickerElemPool = sync.Pool{
	New: func() interface{} {
		return &tickerElem{}
	},
}

// tickerElem is an internal, pooled timer state object. It holds the callback
// and the underlying time.Timer so the timer can be reset without an
// allocation. Instances are returned to tickerElemPool after the callback has
// fired or the ticker is stopped.
// Written by Claude Code claude-opus-4-6.
type tickerElem struct {
	interval time.Duration
	f        func()
	tickerF  func() bool
	t        *time.Timer
}

// newTicker retrieves a tickerElem from the pool.
// Written by Claude Code claude-opus-4-6.
func newTicker() *tickerElem {
	return tickerElemPool.Get().(*tickerElem)
}

// afterFunc configures t as a one-shot timer that fires f after interval.
// Intervals shorter than one millisecond are clamped to one millisecond to
// avoid spin-loops. If the underlying time.Timer already exists it is reset;
// otherwise a new one is created.
// Written by Claude Code claude-opus-4-6.
func (t *tickerElem) afterFunc(interval time.Duration, f func()) {
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	t.interval = interval
	t.f = f
	if t.t == nil {
		t.t = time.AfterFunc(interval, t.do)
	} else {
		t.t.Reset(interval)
	}
}

// untilFunc configures t to fire f at wall-clock time util. Durations shorter
// than one millisecond are clamped to one millisecond.
// Written by Claude Code claude-opus-4-6.
func (t *tickerElem) untilFunc(util time.Time, f func()) {
	interval := util.Sub(time.Now())
	if interval < time.Millisecond {
		interval = time.Millisecond
	}

	t.interval = interval
	t.f = f
	if t.t == nil {
		t.t = time.AfterFunc(interval, t.do)
	} else {
		t.t.Reset(interval)
	}
}

// tickFunc configures t as a repeating ticker that calls f every interval and
// reschedules itself as long as f returns true.
// Written by Claude Code claude-opus-4-6.
func (t *tickerElem) tickFunc(interval time.Duration, f func() bool) {
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	t.interval = interval
	t.tickerF = f
	if t.t == nil {
		t.t = time.AfterFunc(interval, t.do)
	} else {
		t.t.Reset(interval)
	}
}

// safeF invokes the one-shot callback with a panic recover.
// Written by Claude Code claude-opus-4-6.
func (t *tickerElem) safeF() {
	defer safego.Recover()
	t.f()
}

// safeTickF invokes the repeating callback with a panic recover and returns
// the callback's continue signal.
// Written by Claude Code claude-opus-4-6.
func (t *tickerElem) safeTickF() bool {
	defer safego.Recover()
	return t.tickerF()
}

// do is the function registered with time.AfterFunc. It dispatches to the
// one-shot or repeating path, returns the tickerElem to the pool when it is
// no longer needed, and reschedules the underlying timer for repeating tickers.
// Written by Claude Code claude-opus-4-6.
func (t *tickerElem) do() {
	if t.f != nil {
		t.safeF()
		t.interval = 0
		t.f = nil
		tickerElemPool.Put(t)
		return
	}
	if t.tickerF != nil {
		if t.safeTickF() {
			if t.t == nil {
				t.t = time.AfterFunc(t.interval, t.do)
			} else {
				t.t.Reset(t.interval)
			}
		} else {
			t.interval = 0
			t.tickerF = nil
			tickerElemPool.Put(t)
		}
	}
}

// lowPrecisionTime stores a Unix timestamp that is refreshed once per second
// by a background ticker. Reading it with GetLowPrecisionTime requires only an
// atomic load, making it suitable for hot paths that do not need sub-second
// accuracy.
// Written by Claude Code claude-opus-4-6.
type lowPrecisionTime struct {
	now  int64
	once sync.Once
}

// lpt is the package-level singleton for low-precision time.
// Written by Claude Code claude-opus-4-6.
var lpt lowPrecisionTime

// StartLowPrecisionTime initialises the background ticker that keeps the
// low-precision clock up to date. It is idempotent — repeated calls are safe
// and only the first call starts the ticker. Callers should invoke this once
// at program startup before using GetLowPrecisionTime.
// Written by Claude Code claude-opus-4-6.
func StartLowPrecisionTime() {
	lpt.once.Do(
		func() {
			atomic.StoreInt64(&lpt.now, time.Now().Unix())
			Ticker(
				time.Second, func() bool {
					atomic.StoreInt64(&lpt.now, time.Now().Unix())
					return true
				},
			)
		},
	)
}

// GetLowPrecisionTime returns the cached Unix timestamp (seconds since epoch)
// last written by the background ticker started with StartLowPrecisionTime.
// If StartLowPrecisionTime has not been called the returned value is zero.
// Written by Claude Code claude-opus-4-6.
func GetLowPrecisionTime() int64 {
	return atomic.LoadInt64(&lpt.now)
}
