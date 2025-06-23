package timer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ravinggo/game/common/safego"
)

var tickerElemPool = sync.Pool{
	New: func() interface{} {
		return &tickerElem{}
	},
}

type tickerElem struct {
	interval time.Duration
	f        func()
	tickerF  func() bool
	t        *time.Timer
}

func newTicker() *tickerElem {
	return tickerElemPool.Get().(*tickerElem)
}

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

func (t *tickerElem) safeF() {
	defer safego.Recover()
	t.f()
}

func (t *tickerElem) safeTickF() bool {
	defer safego.Recover()
	return t.tickerF()
}

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

type lowPrecisionTime struct {
	now  int64
	once sync.Once
}

var lpt lowPrecisionTime

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

func GetLowPrecisionTime() int64 {
	return atomic.LoadInt64(&lpt.now)
}
