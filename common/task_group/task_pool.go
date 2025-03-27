package task_group

import (
	"sync/atomic"

	"github.com/ravinggo/game/common/safego"
)

// TaskPool dynamic task pool
type TaskPool struct {
	poolSize int64
	c        chan func()
	maxCap   int64
}

// NewTaskPool create a TaskPool
func NewTaskPool(poolSize int64, maxCap int64) *TaskPool {
	tp := &TaskPool{
		c:        make(chan func(), maxCap),
		maxCap:   maxCap,
		poolSize: poolSize,
	}

	return tp
}

// PutForce force put task to TaskPool
func (tp *TaskPool) PutForce(f func()) {
	if atomic.AddInt64(&tp.poolSize, 1) <= tp.maxCap {
		go func() {
			defer safego.Recover()
			defer atomic.AddInt64(&tp.poolSize, -1)
			f()
			for {
				f, ok := <-tp.c
				if !ok {
					return
				}
				f()
			}
		}()
		return
	}
	atomic.AddInt64(&tp.poolSize, -1)
	tp.c <- f
}

// Put task to TaskPool, if task pool is full, return false
func (tp *TaskPool) Put(f func()) bool {
	if atomic.AddInt64(&tp.poolSize, 1) <= tp.maxCap {
		go func() {
			defer safego.Recover()
			defer atomic.AddInt64(&tp.poolSize, -1)

			f()
			for {
				select {
				case f, ok := <-tp.c:
					if !ok {
						return
					}
					f()
				default:
					return
				}
			}
		}()
		return true
	}
	atomic.AddInt64(&tp.poolSize, -1)
	select {
	case tp.c <- f:
		return true
	default:
		return false
	}
}
