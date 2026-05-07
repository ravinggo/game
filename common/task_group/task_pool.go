package task_group

import (
	"sync/atomic"

	"github.com/ravinggo/game/common/safego"
)

// TaskPool is a bounded goroutine pool. It keeps at most poolSize goroutines
// alive at any time; excess tasks are queued on an internal buffered channel
// of capacity maxCap. Idle goroutines pick up queued tasks immediately without
// spawning additional goroutines.
//
// Contrast with TaskGroup: TaskPool offers higher throughput at the cost of
// strict per-key ordering — tasks submitted concurrently may execute in any
// order. Use TaskPool when ordering is not required.
// Written by Claude Code claude-opus-4-6.
type TaskPool struct {
	poolSize int64
	c        chan func()
	maxCap   int64
}

// NewTaskPool creates a TaskPool with the given maximum number of goroutines
// (poolSize) and channel buffer capacity (maxCap). The channel buffer absorbs
// bursts when all goroutines are busy.
// Written by Claude Code claude-opus-4-6.
func NewTaskPool(poolSize int64, maxCap int64) *TaskPool {
	tp := &TaskPool{
		c:        make(chan func(), maxCap),
		maxCap:   maxCap,
		poolSize: poolSize,
	}

	return tp
}

// PutForce submits f to the pool, always making progress. If the pool has
// not yet reached its goroutine limit, a new goroutine is spawned to run f
// and then drain any queued tasks. If the limit has been reached, PutForce
// blocks until a slot opens on the task channel. Compared to Put, PutForce
// never drops a task — use it for critical operations.
// Written by Claude Code claude-opus-4-6.
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

// Put submits f to the pool without blocking. If the pool has not reached its
// goroutine limit, a new goroutine is spawned. Otherwise Put attempts a
// non-blocking send on the task channel and returns false immediately if the
// channel is full, providing back-pressure without blocking the caller.
// Written by Claude Code claude-opus-4-6.
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
