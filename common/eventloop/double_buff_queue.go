package eventloop

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"

	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/timer"
)

// DoubleBuffQueue is a lock-minimised, double-buffered event queue that drives
// a single-goroutine event loop. One buffer accumulates incoming events while
// the other is being drained by the loop goroutine; the two slices are swapped
// under the mutex, keeping contention extremely short.
//
// The zero value is not usable; construct one with NewDoubleBuffQueue.
// Written by Claude Code claude-opus-4-6.
type DoubleBuffQueue struct {
	mu           *sync.Cond
	data         [2][]interface{}
	stopped      int32
	over         chan struct{}
	lockOSThread bool
}

// NewDoubleBuffQueue allocates and initialises a DoubleBuffQueue.
// When lockOSThread is true the internal loop goroutine calls
// runtime.LockOSThread, which is required by some OS-thread-affine resources
// such as OpenGL or certain CGo libraries.
// Written by Claude Code claude-opus-4-6.
func NewDoubleBuffQueue(lockOSThread bool) *DoubleBuffQueue {
	return &DoubleBuffQueue{
		mu:           sync.NewCond(&sync.Mutex{}),
		lockOSThread: lockOSThread,
		over:         make(chan struct{}),
	}
}

// PostEventQueue enqueues event e for delivery to the handler registered via
// Start. If the queue has already been stopped the call is a no-op.
// PostEventQueue is safe for concurrent use by multiple goroutines.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) PostEventQueue(e interface{}) {
	if atomic.LoadInt32(&this_.stopped) != 0 {
		return
	}
	this_.mu.L.Lock()
	this_.data[0] = append(this_.data[0], e)
	this_.mu.L.Unlock()
	this_.mu.Signal()
}

// Start launches the background event-loop goroutine. f is called for every
// posted event. As a convenience, if e is itself a func() it is called
// directly instead of being passed to f, enabling deferred work to be posted
// without a wrapping handler.
//
// Zero or more endF functions may be supplied; they are executed in order
// after the queue has been drained following Stop. Each endF is protected by
// a recover so a panic in one does not prevent the others from running.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) Start(f func(event any), endF ...func()) {
	xf := func(event any) {
		defer safego.Recover()
		switch fx := event.(type) {
		case func():
			fx()
		default:
			f(event)
		}
	}
	var rF []func()
	if len(endF) > 0 {
		for _, v := range endF {
			rF = append(
				rF, func() {
					defer safego.RecoverFunc(
						func(e interface{}) {
							logger.Log.Error().Any("panic info", e).Msg("DoubleBuffQueue end func panic")
						},
					)
					v()
				},
			)
		}
	}
	go this_.start(xf, rF...)
}

// start is the internal loop that runs inside the goroutine spawned by Start.
// It swaps the two data buffers under the lock and then processes the inactive
// buffer without holding the lock, so producers are only blocked for a pointer
// swap. When stopped is set the loop drains both buffers before executing the
// endF callbacks and closing the over channel.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) start(f func(event interface{}), endF ...func()) {
	gid := goid.Get()
	defer func() {
		logger.Log.Warn().Int64("gid", gid).Msg("DoubleBuffQueue closed")
		close(this_.over)
	}()
	logger.Log.Warn().Int64("gid", gid).Msg("DoubleBuffQueue run")
	if this_.lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	for atomic.LoadInt32(&this_.stopped) == 0 {
		this_.mu.L.Lock()
		if len(this_.data[0]) == 0 {
			this_.mu.Wait()
		}
		this_.data[0], this_.data[1] = this_.data[1], this_.data[0]
		this_.mu.L.Unlock()

		for _, v := range this_.data[1] {
			f(v)
		}
		clear(this_.data[1])
		this_.data[1] = this_.data[1][:0]
	}
	for _, v := range this_.data[0] {
		f(v)
	}
	for _, v := range this_.data[1] {
		f(v)
	}
	for _, v := range endF {
		v()
	}
}

// Stop signals the event loop to stop, drains any remaining events, executes
// the endF callbacks registered via Start, and waits until the loop goroutine
// has exited before returning. Callers must not post new events after Stop
// returns.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) Stop() {
	atomic.StoreInt32(&this_.stopped, 1)
	this_.mu.Broadcast()
	<-this_.over
}

// AfterFunc schedules f to be posted into the event queue after the specified
// duration, delegating to the timer package. The callback is executed on the
// event-loop goroutine, not on a timer goroutine.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) AfterFunc(duration time.Duration, f func()) {
	timer.AfterFunc(
		duration, func() {
			this_.PostEventQueue(f)
		},
	)
}

// UntilFunc schedules f to be posted into the event queue at the wall-clock
// time t. If t is in the past, the callback fires within one millisecond.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) UntilFunc(t time.Time, f func()) {
	timer.UntilFunc(
		t, func() {
			this_.PostEventQueue(f)
		},
	)
}

// Ticker schedules f to be invoked on the event-loop goroutine every interval.
// f must return true to continue ticking and false to stop. If the queue is
// already stopped when Ticker is called the call is a no-op.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) Ticker(interval time.Duration, f func() bool) {
	if atomic.LoadInt32(&this_.stopped) != 0 {
		return
	}
	timer.AfterFunc(
		interval, func() {
			this_.PostEventQueue(
				func() {
					ok := f()
					if ok {
						this_.Ticker(interval, f)
					}
				},
			)
		},
	)
}

// Stopped reports whether the queue has been stopped.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue) Stopped() bool {
	return atomic.LoadInt32(&this_.stopped) == 1
}

// DoubleBuffQueue2 is the generic counterpart of DoubleBuffQueue. Instead of
// accepting interface{} events it is parameterised over T, eliminating the
// type-assertion overhead in the handler and allowing the compiler to enforce
// type safety at the call site.
//
// The zero value is not usable; construct one with NewDoubleBuffQueue2.
// Written by Claude Code claude-opus-4-6.
type DoubleBuffQueue2[T any] struct {
	mu           *sync.Cond
	data         [2][]T
	stopped      int32
	over         chan struct{}
	lockOSThread bool
}

// NewDoubleBuffQueue2 allocates and initialises a DoubleBuffQueue2[T].
// When lockOSThread is true the internal loop goroutine pins itself to the
// current OS thread via runtime.LockOSThread.
// Written by Claude Code claude-opus-4-6.
func NewDoubleBuffQueue2[T any](lockOSThread bool) *DoubleBuffQueue2[T] {
	return &DoubleBuffQueue2[T]{
		mu:           sync.NewCond(&sync.Mutex{}),
		lockOSThread: lockOSThread,
		over:         make(chan struct{}),
	}
}

// PostEventQueue enqueues a typed event e for delivery to the handler
// registered via Start. If the queue has already been stopped the call is a
// no-op. Safe for concurrent use by multiple goroutines.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue2[T]) PostEventQueue(e T) {
	if atomic.LoadInt32(&this_.stopped) != 0 {
		return
	}

	this_.mu.L.Lock()
	this_.data[0] = append(this_.data[0], e)
	this_.mu.L.Unlock()
	this_.mu.Signal()
}

// Start launches the background event-loop goroutine. f is called for every
// posted event with a recover guard so panics do not crash the loop. Optional
// endF callbacks are executed in order after the queue drains on Stop.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue2[T]) Start(f func(T), endF ...func()) {
	xf := func(event T) {
		defer safego.Recover()
		f(event)

	}
	var rF []func()
	if len(endF) > 0 {
		for _, v := range endF {
			rF = append(
				rF, func() {
					defer safego.RecoverFunc(
						func(e interface{}) {
							logger.Log.Error().Any("panic info", e).Msg("DoubleBuffQueue2 end func panic")
						},
					)
					v()
				},
			)
		}
	}
	go this_.start(xf, rF...)
}

// start is the internal loop for DoubleBuffQueue2. Its buffer-swap and drain
// strategy mirrors DoubleBuffQueue.start; see that function for a detailed
// explanation.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue2[T]) start(f func(T), endF ...func()) {
	gid := goid.Get()
	defer func() {
		logger.Log.Warn().Int64("gid", gid).Msg("DoubleBuffQueue2 closed")
		close(this_.over)
	}()
	logger.Log.Warn().Int64("gid", gid).Msg("DoubleBuffQueue2 run")
	if this_.lockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	for atomic.LoadInt32(&this_.stopped) == 0 {
		this_.mu.L.Lock()
		if len(this_.data[0]) == 0 {
			this_.mu.Wait()
		}
		this_.data[0], this_.data[1] = this_.data[1], this_.data[0]
		this_.mu.L.Unlock()

		for _, v := range this_.data[1] {
			f(v)
		}
		clear(this_.data[1])
		this_.data[1] = this_.data[1][:0]
	}
	for _, v := range this_.data[0] {
		f(v)
	}
	for _, v := range this_.data[1] {
		f(v)
	}
	for _, v := range endF {
		v()
	}
}

// Stop signals the event loop to stop, drains remaining events, runs endF
// callbacks, and blocks until the loop goroutine exits.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue2[T]) Stop() {
	atomic.StoreInt32(&this_.stopped, 1)
	this_.mu.Broadcast()
	<-this_.over
}

// Stopped reports whether the queue has been stopped.
// Written by Claude Code claude-opus-4-6.
func (this_ *DoubleBuffQueue2[T]) Stopped() bool {
	return atomic.LoadInt32(&this_.stopped) == 1
}
