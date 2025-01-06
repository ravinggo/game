package task_group

import (
	"sync/atomic"

	"github.com/ravinggo/game/common/safego"
)

type TaskPool struct {
	poolSize int64
	c        chan func()
	maxCap   int64
}

func NewTaskPool(poolSize int64, maxCap int64) *TaskPool {
	tp := &TaskPool{
		c:        make(chan func(), maxCap),
		maxCap:   maxCap,
		poolSize: poolSize,
	}

	return tp
}

func (tp *TaskPool) PutForce(f func()) {
	if atomic.AddInt64(&tp.poolSize, 1) <= tp.maxCap {
		safego.Go(
			func() {
				defer func() {
					atomic.AddInt64(&tp.poolSize, -1)
				}()
				f()
				for {
					f, ok := <-tp.c
					if !ok {
						return
					}
					f()
				}
			},
		)
		return
	}
	atomic.AddInt64(&tp.poolSize, -1)
	tp.c <- f
}

func (tp *TaskPool) Put(f func()) bool {
	if atomic.AddInt64(&tp.poolSize, 1) <= tp.maxCap {
		safego.Go(
			func() {
				defer func() {
					atomic.AddInt64(&tp.poolSize, -1)
				}()
				f()
				for {
					f, ok := <-tp.c
					if !ok {
						return
					}
					f()
				}
			},
		)
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
