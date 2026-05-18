// Written by Claude Code claude-opus-4-7.
package eventloop

import (
	"sync"
	"testing"
)

// payload is a small struct used by the typed-queue benchmarks. It exercises
// the generic path without the interface{} boxing that DoubleBuffQueue forces.
type payload struct {
	id    int64
	value int64
}

// BenchmarkDoubleBuffQueue_PostEventQueue_Serial measures the cost of a single
// producer posting events. The timer only covers PostEventQueue; the drain
// happens off-timer via wg.Wait so the consumer is not part of the measurement.
// The queue is multi-producer single-consumer, so a single producer reflects
// the uncontended fast path.
func BenchmarkDoubleBuffQueue_PostEventQueue_Serial(b *testing.B) {
	q := NewDoubleBuffQueue(false)
	var wg sync.WaitGroup
	wg.Add(b.N)
	q.Start(func(event any) {
		wg.Done()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PostEventQueue(i)
	}
	b.StopTimer()

	wg.Wait()
	q.Stop()
}

// BenchmarkDoubleBuffQueue2_PostEventQueue_Serial mirrors the above for the
// generic typed queue.
func BenchmarkDoubleBuffQueue2_PostEventQueue_Serial(b *testing.B) {
	q := NewDoubleBuffQueue2[payload](false)
	var wg sync.WaitGroup
	wg.Add(b.N)
	q.Start(func(event payload) {
		wg.Done()
	})
	p := payload{id: 1, value: 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PostEventQueue(p)
	}
	b.StopTimer()

	wg.Wait()
	q.Stop()
}

// BenchmarkDoubleBuffQueue_PostEventQueue_Parallel measures contended
// multi-producer throughput. RunParallel distributes exactly b.N iterations
// across GOMAXPROCS goroutines, so wg.Add(b.N) is correct.
func BenchmarkDoubleBuffQueue_PostEventQueue_Parallel(b *testing.B) {
	q := NewDoubleBuffQueue(false)
	var wg sync.WaitGroup
	wg.Add(b.N)
	q.Start(func(event any) {
		wg.Done()
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.PostEventQueue(i)
			i++
		}
	})
	b.StopTimer()

	wg.Wait()
	q.Stop()
}

// BenchmarkDoubleBuffQueue2_PostEventQueue_Parallel mirrors the above for the
// generic typed queue.
func BenchmarkDoubleBuffQueue2_PostEventQueue_Parallel(b *testing.B) {
	q := NewDoubleBuffQueue2[payload](false)
	var wg sync.WaitGroup
	wg.Add(b.N)
	q.Start(func(event payload) {
		wg.Done()
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		p := payload{id: 1, value: 2}
		for pb.Next() {
			q.PostEventQueue(p)
		}
	})
	b.StopTimer()

	wg.Wait()
	q.Stop()
}

// BenchmarkDoubleBuffQueue_EndToEnd measures the full post → swap → handler
// pipeline. Unlike the Post-only benchmarks above, wg.Wait is inside the
// timed region so the consumer side is included.
func BenchmarkDoubleBuffQueue_EndToEnd(b *testing.B) {
	q := NewDoubleBuffQueue(false)
	var wg sync.WaitGroup
	wg.Add(b.N)
	q.Start(func(event any) {
		wg.Done()
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PostEventQueue(i)
	}
	wg.Wait()
	b.StopTimer()

	q.Stop()
}

// BenchmarkDoubleBuffQueue2_EndToEnd mirrors the above for the generic typed
// queue. The difference vs. the interface{} variant captures the per-event
// cost of boxing into interface{} on the post side and asserting back on the
// drain side.
func BenchmarkDoubleBuffQueue2_EndToEnd(b *testing.B) {
	q := NewDoubleBuffQueue2[payload](false)
	var wg sync.WaitGroup
	wg.Add(b.N)
	q.Start(func(event payload) {
		wg.Done()
	})
	p := payload{id: 1, value: 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.PostEventQueue(p)
	}
	wg.Wait()
	b.StopTimer()

	q.Stop()
}
