package task_group

import (
	"sync"

	"github.com/ravinggo/game/common/safego"
)

type TaskGroupElem[T any] struct {
	Data T
	Func func()
}

// TaskGroup dynamic task group, can't use MPSC? Only mutex can be used
type TaskGroup[T any] struct {
	data      [2][]TaskGroupElem[T]
	mu        sync.Mutex
	f         func(TaskGroupElem[T])
	isRunning bool
	maxCap    int
	onStop    func()
}

func NewTaskGroup[T any](f func(TaskGroupElem[T]), maxCap int) *TaskGroup[T] {
	if maxCap < 16 {
		maxCap = 16
	}
	tg := &TaskGroup[T]{
		maxCap: maxCap,
		f:      f,
	}
	return tg
}

// SetTaskFunc why add this function? for sync.Pool
func (this_ *TaskGroup[T]) SetTaskFunc(f func(TaskGroupElem[T])) {
	this_.f = f
}

// SetMaxCap why add this function? for sync.Pool
func (this_ *TaskGroup[T]) SetMaxCap(maxCap int) {
	this_.maxCap = maxCap
}

func (this_ *TaskGroup[T]) PutForce(d T, f func()) {
	run := false
	this_.mu.Lock()
	l := len(this_.data[0])
	this_.data[0] = append(this_.data[0], TaskGroupElem[T]{Data: d, Func: f})
	l++
	if l == 1 && !this_.isRunning {
		this_.isRunning = true
		run = true
	}
	this_.mu.Unlock()
	if run {
		safego.Go(
			func() {
				defer func() {
					if this_.onStop != nil {
						this_.onStop()
					}
				}()
				for {
					this_.mu.Lock()
					if !this_.isRunning {
						this_.mu.Unlock()

						return
					}
					this_.data[0], this_.data[1] = this_.data[1], this_.data[0]
					if len(this_.data[1]) == 0 {
						this_.isRunning = false
						this_.mu.Unlock()
						return
					}
					this_.mu.Unlock()

					for _, v := range this_.data[1] {
						this_.f(v)
					}

					if len(this_.data[1]) > this_.maxCap*2 {
						this_.data[1] = make([]TaskGroupElem[T], 0, this_.maxCap*2)
					} else {
						clear(this_.data[1])
						this_.data[1] = this_.data[1][:0]
					}
				}
			},
		)
	}
}

func (this_ *TaskGroup[T]) Put(d T, f func()) bool {
	run := false
	this_.mu.Lock()
	l := len(this_.data[0])
	if this_.maxCap != 0 && l >= this_.maxCap {
		this_.mu.Unlock()
		return false
	}
	this_.data[0] = append(this_.data[0], TaskGroupElem[T]{d, f})
	l++
	if l == 1 && !this_.isRunning {
		this_.isRunning = true
		run = true
	}
	this_.mu.Unlock()
	if run {
		safego.Go(
			func() {
				defer func() {
					if this_.onStop != nil {
						this_.onStop()
					}
				}()
				for {
					this_.mu.Lock()
					if !this_.isRunning {
						this_.mu.Unlock()
						return
					}
					this_.data[0], this_.data[1] = this_.data[1], this_.data[0]
					if len(this_.data[1]) == 0 {
						this_.isRunning = false
						this_.mu.Unlock()
						return
					}
					this_.mu.Unlock()

					for _, v := range this_.data[1] {
						this_.f(v)
					}

					if len(this_.data[1]) > this_.maxCap*2 {
						this_.data[1] = make([]TaskGroupElem[T], 0, this_.maxCap*2)
					} else {
						clear(this_.data[1])
						this_.data[1] = this_.data[1][:0]
					}
				}
			},
		)
	}
	return true
}

func (this_ *TaskGroup[T]) IsRunning() bool {
	this_.mu.Lock()
	isRunning := this_.isRunning
	this_.mu.Unlock()
	return isRunning
}
