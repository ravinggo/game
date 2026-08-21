// Tests for FixedHashPoolService — unit and concurrency.
// Written by Claude Code claude-opus-4-6.
package service

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/task_group"
)

// makeFHPS constructs a FixedHashPoolService without a NATS connection.
// Written by Claude Code claude-opus-4-6.
func makeFHPS(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace]) *FixedHashPoolService[ctx.IntTrace, *ctx.IntTrace] {
	s := &FixedHashPoolService[ctx.IntTrace, *ctx.IntTrace]{
		BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
			ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
			h:       h,
		},
	}

	numCpu := uint64(runtime.NumCPU())
	if numCpu&1 == 1 {
		numCpu++
	}
	poolSize := numCpu * 1024
	s.taskPoolMark = poolSize - 1
	s.taskGroupHash = make([]task_group.TaskGroup[CE[ctx.IntTrace, *ctx.IntTrace]], poolSize)
	for i := range s.taskGroupHash {
		s.taskGroupHash[i].SetMaxCap(128)
		s.taskGroupHash[i].SetTaskFunc(s.taskFunc)
	}
	s.dispatch = s.doDispatch
	return s
}

// postFHPSEvent dispatches one event with the given RoleId.
// Written by Claude Code claude-opus-4-6.
func postFHPSEvent(s *FixedHashPoolService[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace], roleID int64) {
	c := s.GetCtxFromPool()
	r, _ := elem.Acquire()
	r.(*basepb.IntTrace).RoleId = roleID
	c.Req = r
	c.GetTrace().SetRoleID(roleID)
	s.dispatch(c, elem)
}

// ── unit tests ──────────────────────────────────────────────────────────────

// TestFixedHashPoolService_DispatchCallsHandler verifies a dispatched event
// reaches the registered handler.
// Written by Claude Code claude-opus-4-6.
func TestFixedHashPoolService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	h.RegisterEvent("fhps-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		close(done)
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFHPS(h)
	postFHPSEvent(s, elem, 42)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called within timeout")
	}
}

// TestFixedHashPoolService_SameHash_Sequential verifies that events with the same
// roleID (and thus the same hash) land in the same bucket and are processed sequentially.
// Written by Claude Code claude-opus-4-6.
func TestFixedHashPoolService_SameHash_Sequential(t *testing.T) {
	const N = 20
	const roleID int64 = 13 // fixed roleID → same TaskGroup bucket always
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var seq atomic.Int64
	results := make([]int64, 0, N)
	var mu sync.Mutex
	allDone := make(chan struct{})
	var count int

	h.RegisterEvent("fhps-seq", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		v := seq.Add(1)
		mu.Lock()
		results = append(results, v)
		count++
		if count == N {
			close(allDone)
		}
		mu.Unlock()
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFHPS(h)
	for i := 0; i < N; i++ {
		postFHPSEvent(s, elem, roleID)
	}

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out after %d/%d events", count, N)
	}

	for i, v := range results {
		if v != int64(i+1) {
			t.Fatalf("ordering violation at index %d: got %d, want %d", i, v, i+1)
		}
	}
}

// TestFixedHashPoolService_ZeroRoleID_Dispatches verifies that roleID==0 is randomly
// assigned to a valid bucket instead of causing a panic or drop.
// Written by Claude Code claude-opus-4-6.
func TestFixedHashPoolService_ZeroRoleID_Dispatches(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	h.RegisterEvent("fhps-zero", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		close(done)
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFHPS(h)
	c := s.GetCtxFromPool()
	c.Req, _ = elem.Acquire()
	s.dispatch(c, elem)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("zero-hash dispatch not delivered within timeout")
	}
}

// TestFixedHashPoolService_PostTask_Runs verifies that PostTask delivers a
// func to the appropriate hash bucket's TaskGroup using a fresh internal ctx.
// Written by Claude Code claude-opus-4-6.
func TestFixedHashPoolService_PostTask_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFHPS(h)

	c := s.GetCtxFromPool()
	c.GetTrace().(*ctx.IntTrace).RoleId = 55
	ran := make(chan struct{})
	s.PostTaskCloneCtx(
		c, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			close(ran)
			return nil
		},
	)
	s.PutCtxToPool(c)

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("PostTask func did not run within timeout")
	}
}

// TestFixedHashPoolService_PostTaskWithRoleId_Runs verifies that PostTaskWithRoleId delivers the func
// and stamps the correct RoleId onto the ctx passed to it.
func TestFixedHashPoolService_PostTaskWithRoleId_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFHPS(h)

	ran := make(chan int64, 1)
	s.PostTaskWithRoleId(
		33, func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			ran <- c.GetTrace().GetRoleID()
			return nil
		},
	)

	select {
	case got := <-ran:
		if got != 33 {
			t.Fatalf("expected roleId 33, got %d", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PostTaskWithRoleId did not run within timeout")
	}
}

// ── concurrency tests ────────────────────────────────────────────────────────

// TestFixedHashPoolService_ConcurrentDispatch posts events from many goroutines
// across many distinct hashes and verifies all are delivered without loss or deadlock.
// Written by Claude Code claude-opus-4-6.
func TestFixedHashPoolService_ConcurrentDispatch(t *testing.T) {
	const goroutines = 20
	const eventsEach = 30
	const total = goroutines * eventsEach

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var received atomic.Int64
	allDone := make(chan struct{})

	h.RegisterEvent("fhps-concurrent", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		if received.Add(1) == int64(total) {
			close(allDone)
		}
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFHPS(h)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			roleID := int64(g + 1) // distinct non-zero roleID → distinct hash per goroutine
			for i := 0; i < eventsEach; i++ {
				postFHPSEvent(s, elem, roleID)
			}
		}()
	}
	wg.Wait()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d/%d events delivered", received.Load(), total)
	}
}

// TestFixedHashPoolService_SameHash_NoRace verifies that the same-bucket TaskGroup
// never processes two messages concurrently, even under concurrent posting pressure.
// RegisterEventForce is used so that PutForce bypasses backpressure and no events
// are dropped regardless of how many goroutines are posting simultaneously.
// Written by Claude Code claude-opus-4-6.
func TestFixedHashPoolService_SameHash_NoRace(t *testing.T) {
	const goroutines = 10
	const eventsEach = 10
	const total = goroutines * eventsEach

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var received atomic.Int64
	allDone := make(chan struct{})

	// Force registration bypasses Put() backpressure so the test is not affected by
	// queue-full drops when many goroutines post to the same bucket simultaneously.
	h.RegisterEventForce("fhps-norace", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(500 * time.Microsecond)
		inFlight.Add(-1)
		if received.Add(1) == int64(total) {
			close(allDone)
		}
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFHPS(h)

	const roleID int64 = 3 // all goroutines use the same roleID → hash=3 → same bucket
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < eventsEach; i++ {
				postFHPSEvent(s, elem, roleID)
			}
		}()
	}
	wg.Wait()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d/%d events delivered", received.Load(), total)
	}

	if maxInFlight.Load() > 1 {
		t.Errorf("same-bucket handler ran concurrently: max in-flight = %d, want 1", maxInFlight.Load())
	}
}
