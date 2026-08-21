// Tests for TaskPoolService — unit and concurrency.
// Written by Claude Code claude-opus-4-6.
package service

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/task_group"
	"github.com/ravinggo/objectpool"
)

// makeTPS constructs a TaskPoolService without a NATS connection.
// Written by Claude Code claude-opus-4-6.
func makeTPS(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace]) *TaskPoolService[ctx.IntTrace, *ctx.IntTrace] {
	numCpu := uint64(runtime.NumCPU())
	if numCpu&1 == 1 {
		numCpu++
	}
	poolSize := int64(numCpu * 1024)
	s := &TaskPoolService[ctx.IntTrace, *ctx.IntTrace]{
		BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
			ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
			h:       h,
		},
		taskPool: task_group.NewTaskPool(poolSize, poolSize*10),
	}
	s.dispatch = s.doDispatch
	return s
}

// postTPSEvent dispatches one event with the given RoleId payload (hash is ignored by TaskPool).
// Written by Claude Code claude-opus-4-6.
func postTPSEvent(s *TaskPoolService[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace], roleID int64) {
	c := s.GetCtxFromPool()
	r, _ := elem.Acquire()
	r.(*basepb.IntTrace).RoleId = roleID
	c.Req = r
	s.dispatch(c, elem)
}

// ── unit tests ──────────────────────────────────────────────────────────────

// TestTaskPoolService_DispatchCallsHandler verifies that a dispatched event
// reaches the registered handler.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	h.RegisterEvent("tps-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		close(done)
		return nil
	})
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeTPS(h)
	postTPSEvent(s, elem, 1)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called within timeout")
	}
}

// TestTaskPoolService_PostTask_Runs verifies that PostTask delivers an arbitrary
// function to the worker pool using a fresh internal ctx.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_PostTask_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeTPS(h)

	c := s.GetCtxFromPool()
	c.GetTrace().(*ctx.IntTrace).RoleId = 5
	ran := make(chan struct{})
	s.PostTaskCloneCtx(c, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
		close(ran)
		return nil
	})
	s.PutCtxToPool(c)

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("PostTask func did not run within timeout")
	}
}

// TestTaskPoolService_PostTaskWithRoleId_Runs verifies that PostTaskWithRoleId delivers the func
// and stamps the correct RoleId onto the ctx passed to it.
func TestTaskPoolService_PostTaskWithRoleId_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeTPS(h)

	ran := make(chan int64, 1)
	s.PostTaskWithRoleId(99, func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
		ran <- c.GetTrace().GetRoleID()
		return nil
	})

	select {
	case got := <-ran:
		if got != 99 {
			t.Fatalf("expected roleId 99, got %d", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PostTaskWithRoleId did not run within timeout")
	}
}

// TestTaskPoolService_PostTask_NilIsNoop verifies that passing nil args to PostTask
// is a silent no-op.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_PostTask_NilIsNoop(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeTPS(h)
	c := s.GetCtxFromPool()
	s.PostTaskCloneCtx(c, nil) // nil func — must not panic
	s.PostTaskCloneCtx(nil, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg { return nil }) // nil ctx — must not panic
	s.PutCtxToPool(c)
}

// TestTaskPoolService_HandlerError_NoNatsMsgNoReply verifies that a handler returning
// an error does not panic when NatsMsg is nil.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_HandlerError_NoNatsMsgNoReply(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	h.RegisterEvent("tps-err", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.ErrorMessage) *berror.ErrMsg {
		defer close(done)
		return berror.NewProtocolStr("intentional error")
	})
	elem, _ := h.Lookup("basepb.ErrorMessage")

	s := makeTPS(h)
	c := s.GetCtxFromPool()
	c.Req, _ = elem.Acquire()
	s.dispatch(c, elem)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("error handler did not run within timeout")
	}
}

// ── concurrency tests ────────────────────────────────────────────────────────

// TestTaskPoolService_ConcurrentDispatch_AllDelivered posts events from many goroutines
// simultaneously and verifies all are eventually processed without deadlock or loss.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_ConcurrentDispatch_AllDelivered(t *testing.T) {
	const goroutines = 20
	const eventsEach = 50
	const total = goroutines * eventsEach

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var received atomic.Int64
	allDone := make(chan struct{})

	h.RegisterEvent("tps-concurrent", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		if received.Add(1) == int64(total) {
			close(allDone)
		}
		return nil
	})
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeTPS(h)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < eventsEach; i++ {
				postTPSEvent(s, elem, int64(g*eventsEach+i))
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

// TestTaskPoolService_TrueParallelism verifies that the TaskPool can execute multiple
// handlers concurrently — i.e., a slow handler does not block fast handlers.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_TrueParallelism(t *testing.T) {
	const N = 4

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	barrier := make(chan struct{}) // all N goroutines must reach this point simultaneously
	var arrivals atomic.Int32
	allArrived := make(chan struct{})

	h.RegisterEvent("tps-parallel", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		if arrivals.Add(1) == N {
			close(allArrived)
		}
		<-barrier // all N handlers block here simultaneously
		return nil
	})
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeTPS(h)

	for i := 0; i < N; i++ {
		postTPSEvent(s, elem, int64(i))
	}

	// Wait for all N handlers to be running simultaneously.
	select {
	case <-allArrived:
		// N handlers are now blocked at <-barrier simultaneously → confirmed parallelism.
		close(barrier)
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d/%d handlers reached the barrier; TaskPool may not be running handlers in parallel", arrivals.Load(), N)
	}
}

// TestTaskPoolService_ConcurrentPostTask verifies that multiple goroutines can
// safely call PostTask concurrently without data races.
// Written by Claude Code claude-opus-4-6.
func TestTaskPoolService_ConcurrentPostTask(t *testing.T) {
	const goroutines = 30
	const tasksEach = 20
	const total = goroutines * tasksEach

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeTPS(h)

	var received atomic.Int64
	allDone := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < tasksEach; i++ {
				c := s.GetCtxFromPool()
				s.PostTaskCloneCtx(c, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
					if received.Add(1) == int64(total) {
						close(allDone)
					}
					return nil
				})
				s.PutCtxToPool(c)
			}
		}()
	}
	wg.Wait()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d/%d PostTask tasks completed", received.Load(), total)
	}
}
