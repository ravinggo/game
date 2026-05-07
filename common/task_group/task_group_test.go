// Written by Claude Code claude-opus-4-6.
package task_group

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestTaskGroup_PutForce_EnqueuesAtCapacity verifies that PutForce enqueues
// tasks even when the group is already at its stated capacity.
func TestTaskGroup_PutForce_EnqueuesAtCapacity(t *testing.T) {
	const cap = 16
	var processed int64
	barrier := make(chan struct{})

	tg := NewTaskGroup[int](func(elem TaskGroupElem[int]) {
		// Block until the test releases the barrier so we can fill the queue.
		<-barrier
		atomic.AddInt64(&processed, 1)
	}, cap)

	// Fill the queue beyond its capacity using PutForce.
	const n = cap * 2
	for i := 0; i < n; i++ {
		tg.PutForce(i, nil)
	}

	// Release the barrier so tasks can drain.
	close(barrier)

	// Wait for all tasks to drain.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= int64(n) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt64(&processed); got < int64(n) {
		t.Fatalf("PutForce: expected at least %d tasks processed, got %d", n, got)
	}
}

// TestTaskGroup_Put_ReturnsFalseWhenFull verifies that Put returns false when
// the pending task count has reached maxCap.
func TestTaskGroup_Put_ReturnsFalseWhenFull(t *testing.T) {
	const cap = 16
	barrier := make(chan struct{})

	tg := NewTaskGroup[int](func(elem TaskGroupElem[int]) {
		<-barrier
	}, cap)

	// Fill to capacity. The first accepted task starts the goroutine which
	// immediately blocks on barrier, so subsequent puts stack up.
	accepted := 0
	for i := 0; i < cap*3; i++ {
		if tg.Put(i, nil) {
			accepted++
		}
	}

	// At least one Put must have returned false because the queue is bounded.
	if accepted >= cap*3 {
		t.Fatal("Put never returned false even though cap was exceeded")
	}

	// Release the barrier so the test doesn't leak goroutines.
	close(barrier)
}

// TestTaskGroup_TaskFunc_CalledWithCorrectData verifies that the handler
// receives the exact data value that was supplied to Put.
func TestTaskGroup_TaskFunc_CalledWithCorrectData(t *testing.T) {
	type payload struct{ id int }

	received := make(chan payload, 1)
	tg := NewTaskGroup[payload](func(elem TaskGroupElem[payload]) {
		received <- elem.Data
	}, 16)

	want := payload{id: 99}
	tg.Put(want, nil)

	select {
	case got := <-received:
		if got != want {
			t.Fatalf("handler received %v, want %v", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not called within timeout")
	}
}

// TestTaskGroup_PutForce_ConcurrentSafety verifies that multiple goroutines
// can call PutForce simultaneously without data races or panics.
func TestTaskGroup_PutForce_ConcurrentSafety(t *testing.T) {
	const goroutines = 8
	const tasksEach = 50

	var processed int64
	tg := NewTaskGroup[int](func(elem TaskGroupElem[int]) {
		atomic.AddInt64(&processed, 1)
	}, 16)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < tasksEach; i++ {
				tg.PutForce(id*tasksEach+i, nil)
			}
		}(g)
	}
	wg.Wait()

	// Wait for all tasks to drain.
	deadline := time.Now().Add(3 * time.Second)
	want := int64(goroutines * tasksEach)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&processed) >= want {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := atomic.LoadInt64(&processed); got < want {
		t.Fatalf("concurrent PutForce: expected %d tasks, got %d", want, got)
	}
}
