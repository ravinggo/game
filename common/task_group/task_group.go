package task_group

import (
	"sync"

	"github.com/ravinggo/game/common/safego"
)

// TaskGroupElem is a single work item held by a TaskGroup. Data carries
// caller-supplied metadata (e.g. a user-id or session object) that is
// forwarded to the handler function alongside the action to execute.
// Written by Claude Code claude-opus-4-6.
type TaskGroupElem[T any] struct {
	Data T
	Func func()
}

// TaskGroup is a dynamic, per-key serialisation primitive. All tasks submitted
// to the same TaskGroup instance are executed sequentially in a single
// goroutine; a new goroutine is spawned automatically when the first task
// arrives and exits as soon as the backlog is empty, keeping idle memory
// overhead minimal.
//
// TaskGroup is safe for concurrent Put / PutForce calls but is NOT designed as
// a general-purpose MPSC queue — it relies on a mutex rather than a lock-free
// ring. Use it when per-entity ordering is more important than raw throughput.
//
// The zero value is not usable; construct one with NewTaskGroup.
// Written by Claude Code claude-opus-4-6.
type TaskGroup[T any] struct {
	data      [2][]TaskGroupElem[T]
	mu        sync.Mutex
	f         func(TaskGroupElem[T])
	isRunning bool
	maxCap    int
	meta      int64
}

// NewTaskGroup allocates a TaskGroup with the given handler and maximum
// pending-task capacity. maxCap is clamped to a minimum of 16. The handler f
// is called for every TaskGroupElem in FIFO order on a dedicated goroutine; it
// is wrapped in a recover so a panic does not terminate the goroutine.
// Written by Claude Code claude-opus-4-6.
func NewTaskGroup[T any](f func(TaskGroupElem[T]), maxCap int) *TaskGroup[T] {
	if maxCap < 16 {
		maxCap = 16
	}
	tg := &TaskGroup[T]{
		maxCap: maxCap,
		f: func(t TaskGroupElem[T]) {
			defer safego.Recover()
			f(t)
		},
	}
	return tg
}

// SetTaskFunc replaces the task handler. This is provided primarily to support
// reuse of TaskGroup instances obtained from a sync.Pool — it must not be
// called while the group has tasks in flight.
// Written by Claude Code claude-opus-4-6.
func (this_ *TaskGroup[T]) SetTaskFunc(f func(TaskGroupElem[T])) {
	this_.f = f
}

// SetMaxCap updates the maximum pending-task capacity. Like SetTaskFunc this
// exists to support sync.Pool reuse and must not be called concurrently with
// Put / PutForce.
// Written by Claude Code claude-opus-4-6.
func (this_ *TaskGroup[T]) SetMaxCap(maxCap int) {
	this_.maxCap = maxCap
}

// PutForce enqueues a task unconditionally, bypassing the capacity limit. This
// is the preferred path for critical operations (e.g. payment processing) where
// dropping the task would be worse than allowing the queue to grow beyond its
// soft limit.
//
// If no goroutine is currently running for this group, PutForce spawns one.
// Written by Claude Code claude-opus-4-6.
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
		go func() {
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
		}()
	}
}

// Put attempts to enqueue a task. It returns false without enqueueing if the
// pending task count has reached maxCap, providing back-pressure to callers.
// If no goroutine is currently running for this group and the task is
// accepted, Put spawns one.
// Written by Claude Code claude-opus-4-6.
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
		go func() {
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
		}()
	}
	return true
}

// IsRunning reports whether the internal drain goroutine is currently active.
// A return value of false means the group is idle and the next Put / PutForce
// will spawn a new goroutine.
// Written by Claude Code claude-opus-4-6.
func (this_ *TaskGroup[T]) IsRunning() bool {
	this_.mu.Lock()
	isRunning := this_.isRunning
	this_.mu.Unlock()
	return isRunning
}
