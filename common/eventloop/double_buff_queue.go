package eventloop

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"

	"github.com/petermattis/goid"
)

type DoubleBuffQueue struct {
	mu           *sync.Cond
	data         [2][]interface{}
	stopped      int32
	over         chan struct{}
	lockOSThread bool
}

func NewDoubleBuffQueue(lockOSThread bool) *DoubleBuffQueue {
	return &DoubleBuffQueue{
		mu:           sync.NewCond(&sync.Mutex{}),
		lockOSThread: lockOSThread,
		over:         make(chan struct{}),
	}
}

func (this_ *DoubleBuffQueue) PostEventQueue(e interface{}) {
	if atomic.LoadInt32(&this_.stopped) != 0 {
		return
	}
	this_.mu.L.Lock()
	this_.data[0] = append(this_.data[0], e)
	this_.mu.L.Unlock()
	this_.mu.Signal()
}

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

func (this_ *DoubleBuffQueue) Stop() {
	atomic.StoreInt32(&this_.stopped, 1)
	this_.mu.Broadcast()
	<-this_.over
}

func (this_ *DoubleBuffQueue) Stopped() bool {
	return atomic.LoadInt32(&this_.stopped) == 1
}

type DoubleBuffQueue2[T any] struct {
	mu           *sync.Cond
	data         [2][]T
	stopped      int32
	over         chan struct{}
	lockOSThread bool
}

func NewDoubleBuffQueue2[T any](lockOSThread bool) *DoubleBuffQueue2[T] {
	return &DoubleBuffQueue2[T]{
		mu:           sync.NewCond(&sync.Mutex{}),
		lockOSThread: lockOSThread,
		over:         make(chan struct{}),
	}
}

func (this_ *DoubleBuffQueue2[T]) PostEventQueue(e T) {
	if atomic.LoadInt32(&this_.stopped) != 0 {
		return
	}

	this_.mu.L.Lock()
	this_.data[0] = append(this_.data[0], e)
	this_.mu.L.Unlock()
	this_.mu.Signal()
}

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

func (this_ *DoubleBuffQueue2[T]) Stop() {
	atomic.StoreInt32(&this_.stopped, 1)
	this_.mu.Broadcast()
	<-this_.over
}

func (this_ *DoubleBuffQueue2[T]) Stopped() bool {
	return atomic.LoadInt32(&this_.stopped) == 1
}
