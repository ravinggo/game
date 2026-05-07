// Written by Claude Code claude-opus-4-6.
package task_group

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTaskPool_Put_DeliversToWorkers verifies that tasks submitted via Put are
// executed by the pool workers.
func TestTaskPool_Put_DeliversToWorkers(t *testing.T) {
	var count int64
	tp := NewTaskPool(4, 64)

	const n = 20
	for i := 0; i < n; i++ {
		tp.Put(func() {
			atomic.AddInt64(&count, 1)
		})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&count) >= int64(n) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt64(&count); got < int64(n) {
		t.Fatalf("Put: expected %d tasks executed, got %d", n, got)
	}
}

// TestTaskPool_PutForce_AlwaysDelivers verifies that PutForce never drops a
// task, even when the pool is at capacity.
func TestTaskPool_PutForce_AlwaysDelivers(t *testing.T) {
	var count int64
	// Very small pool so we exercise the channel-blocking path in PutForce.
	tp := NewTaskPool(2, 32)

	const n = 40
	for i := 0; i < n; i++ {
		tp.PutForce(func() {
			atomic.AddInt64(&count, 1)
		})
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&count) >= int64(n) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt64(&count); got < int64(n) {
		t.Fatalf("PutForce: expected %d tasks executed, got %d", n, got)
	}
}

// TestTaskPool_ConcurrentPuts verifies that multiple goroutines can submit
// tasks concurrently without data races or dropped work when using PutForce.
func TestTaskPool_ConcurrentPuts(t *testing.T) {
	var count int64
	tp := NewTaskPool(8, 256)

	const goroutines = 10
	const tasksEach = 20

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < tasksEach; i++ {
				tp.PutForce(func() {
					atomic.AddInt64(&count, 1)
				})
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * tasksEach)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&count) >= want {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt64(&count); got < want {
		t.Fatalf("concurrent puts: expected %d tasks executed, got %d", want, got)
	}
}
