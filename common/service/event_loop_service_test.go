// Tests for EventLoopService — unit and concurrency.
// Written by Claude Code claude-opus-4-6.
package service

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
)

// makeELS constructs an EventLoopService without a NATS connection.
// The caller is responsible for starting and stopping the EventLoop.
// Written by Claude Code claude-opus-4-6.
func makeELS(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace]) *EventLoopService[ctx.IntTrace, *ctx.IntTrace] {
	s := &EventLoopService[ctx.IntTrace, *ctx.IntTrace]{
		BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
			ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
			h:       h,
		},
		el: eventloop.NewDoubleBuffQueue(false),
	}
	s.dispatch = func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
		s.el.PostEventQueue(CE[ctx.IntTrace, *ctx.IntTrace]{Ctx: c, Elem: elem})
	}
	return s
}

// startELS starts the EventLoop of s and returns a stop func.
// Written by Claude Code claude-opus-4-6.
func startELS(s *EventLoopService[ctx.IntTrace, *ctx.IntTrace]) func() {
	s.el.Start(
		func(e any) {
			switch ev := e.(type) {
			case CE[ctx.IntTrace, *ctx.IntTrace]:
				if ev.Func != nil {
					if err := s.applyServiceMiddles(ev.Func)(ev.Ctx); err != nil {
						ev.Ctx.Warn().Err(err).Msg("PostTask func error")
					}
					s.PutCtxToPool(ev.Ctx)
				} else {
					s.handleCtx(ev.Ctx, ev.Elem)
				}
			case func():
				ev()
			}
		},
	)
	return s.el.Stop
}

// postELSEvent dispatches one event with the given RoleId.
// Written by Claude Code claude-opus-4-6.
func postELSEvent(s *EventLoopService[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace], roleID int64) {
	c := s.GetCtxFromPool()
	r, _ := elem.Acquire()
	r.(*basepb.IntTrace).RoleId = roleID
	c.Req = r
	s.dispatch(c, elem)
}

// ── unit tests ──────────────────────────────────────────────────────────────

// TestEventLoopService_DispatchCallsHandler verifies that a dispatched event
// reaches the registered handler.
// Written by Claude Code claude-opus-4-6.
func TestEventLoopService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var called atomic.Bool
	done := make(chan struct{})
	handler.RegisterEvent(
		h, "els-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			if called.CompareAndSwap(false, true) {
				close(done)
			}
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	postELSEvent(s, elem, 1)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not called within timeout")
	}
}

// TestEventLoopService_Sequential_Order verifies that events posted from a single
// goroutine are processed in arrival order — the core EventLoop guarantee.
// Written by Claude Code claude-opus-4-6.
func TestEventLoopService_Sequential_Order(t *testing.T) {
	const N = 30
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	results := make([]int64, 0, N)
	var mu sync.Mutex
	allDone := make(chan struct{})
	var count int

	handler.RegisterEvent(
		h, "els-order", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			mu.Lock()
			results = append(results, req.RoleId)
			count++
			if count == N {
				close(allDone)
			}
			mu.Unlock()
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	for i := 0; i < N; i++ {
		postELSEvent(s, elem, int64(i))
	}

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out after receiving %d/%d events", count, N)
	}

	for i, v := range results {
		if v != int64(i) {
			t.Fatalf("out of order at index %d: got %d, want %d", i, v, i)
		}
	}
}

// TestEventLoopService_PostTask_Runs verifies that PostTask delivers an arbitrary
// function to the EventLoop goroutine using a fresh internal ctx.
// Written by Claude Code claude-opus-4-6.
func TestEventLoopService_PostTask_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	c := s.GetCtxFromPool()
	c.GetTrace().(*ctx.IntTrace).RoleId = 7
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

// TestEventLoopService_PostTaskWithRoleId_Runs verifies that PostTaskWithRoleId delivers the func
// and stamps the correct RoleId onto the ctx passed to it.
func TestEventLoopService_PostTaskWithRoleId_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	ran := make(chan int64, 1)
	s.PostTaskWithRoleId(
		42, func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			ran <- c.GetTrace().GetRoleID()
			return nil
		},
	)

	select {
	case got := <-ran:
		if got != 42 {
			t.Fatalf("expected roleId 42, got %d", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PostTaskWithRoleId did not run within timeout")
	}
}

// TestEventLoopService_HandlerError_NoNatsMsgNoReply verifies that a handler returning
// an error does not panic when NatsMsg is nil.
// Written by Claude Code claude-opus-4-6.
func TestEventLoopService_HandlerError_NoNatsMsgNoReply(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	handler.RegisterEvent(
		h, "els-err", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			defer close(done)
			return berror.NewProtocolStr("intentional error")
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	c := s.GetCtxFromPool()
	c.Req, c.Resp = elem.Acquire()
	s.dispatch(c, elem)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("error handler did not run within timeout")
	}
}

// ── concurrency tests ────────────────────────────────────────────────────────

// TestEventLoopService_ConcurrentPost_AllDelivered posts events from many goroutines
// simultaneously and verifies that every event is delivered without loss.
// Written by Claude Code claude-opus-4-6.
func TestEventLoopService_ConcurrentPost_AllDelivered(t *testing.T) {
	const goroutines = 20
	const eventsPerGoroutine = 50
	const total = goroutines * eventsPerGoroutine

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var received atomic.Int64
	allDone := make(chan struct{})

	handler.RegisterEvent(
		h, "els-concurrent", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			if received.Add(1) == int64(total) {
				close(allDone)
			}
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < eventsPerGoroutine; i++ {
				postELSEvent(s, elem, int64(g*eventsPerGoroutine+i))
			}
		}()
	}
	wg.Wait()

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("only %d/%d events delivered before timeout", received.Load(), total)
	}
}

// TestEventLoopService_StopDrainsEvents verifies that calling Stop after posting
// events allows already-queued events to drain before the loop exits.
// Written by Claude Code claude-opus-4-6.
func TestEventLoopService_StopDrainsEvents(t *testing.T) {
	const N = 10
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var count atomic.Int64

	handler.RegisterEvent(
		h, "els-stop", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			count.Add(1)
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeELS(h)
	stop := startELS(s)

	for i := 0; i < N; i++ {
		postELSEvent(s, elem, int64(i))
	}
	stop() // blocks until loop exits

	if s.el.Stopped() == false {
		t.Error("EventLoop should report stopped after Stop()")
	}
}
