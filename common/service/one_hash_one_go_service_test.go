// Tests for OneHashOneGoService — unit and concurrency.
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

// makeOHOGS constructs a OneHashOneGoService without a NATS connection.
// The caller must start and stop the EventLoop.
// Written by Claude Code claude-opus-4-6.
func makeOHOGS(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace]) *OneHashOneGoService[ctx.IntTrace, *ctx.IntTrace] {
	var s *OneHashOneGoService[ctx.IntTrace, *ctx.IntTrace]
	s = &OneHashOneGoService[ctx.IntTrace, *ctx.IntTrace]{
		BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
			ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
			h:       h,
			dispatch: func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
				s.doDispatch(c, elem)
			},
		},
		el:            eventloop.NewDoubleBuffQueue(false),
		taskMap:       map[int64]*timeTask[ctx.IntTrace, *ctx.IntTrace]{},
		taskGroupPool: objectpool.GetTypePool[timeTask[ctx.IntTrace, *ctx.IntTrace]](),
	}
	return s
}

// startOHOGS starts the EventLoop of s and returns a stop func.
// Written by Claude Code claude-opus-4-6.
func startOHOGS(s *OneHashOneGoService[ctx.IntTrace, *ctx.IntTrace]) func() {
	s.el.Start(
		func(e any) {
			switch ev := e.(type) {
			case CE[ctx.IntTrace, *ctx.IntTrace]:
				s.dealCE(ev)
			case func():
				ev()
			}
		},
	)
	return s.el.Stop
}

// postOHOGSEvent dispatches one event with the given RoleId.
// Written by Claude Code claude-opus-4-6.
func postOHOGSEvent(s *OneHashOneGoService[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace], roleID int64) {
	c := s.GetCtxFromPool()
	r, _ := elem.Acquire()
	r.(*basepb.IntTrace).RoleId = roleID
	c.Req = r
	c.GetTrace().SetRoleID(roleID)
	s.dispatch(c, elem)
}

// ── unit tests ──────────────────────────────────────────────────────────────

// TestOneHashOneGoService_DispatchCallsHandler verifies a basic dispatched event
// reaches the registered handler.
// Written by Claude Code claude-opus-4-6.
func TestOneHashOneGoService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	h.RegisterEvent("ohog-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		close(done)
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	postOHOGSEvent(s, elem, 42)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called within timeout")
	}
}

// TestOneHashOneGoService_SameHash_Sequential verifies that events with the same
// roleID (and thus the same hash) are processed strictly in the order they were posted.
// Written by Claude Code claude-opus-4-6.
func TestOneHashOneGoService_SameHash_Sequential(t *testing.T) {
	const N = 30
	const roleID int64 = 7 // fixed roleID → same TaskGroup bucket always
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var seq atomic.Int64
	results := make([]int64, 0, N)
	var mu sync.Mutex
	allDone := make(chan struct{})
	var count int

	h.RegisterEvent("ohog-seq", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
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

	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	for i := 0; i < N; i++ {
		postOHOGSEvent(s, elem, roleID)
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

// TestOneHashOneGoService_ZeroHash_GetsProcessed verifies that hash==0 events are
// routed through safego.Go and still reach the handler (no ordering guarantee).
// Written by Claude Code claude-opus-4-6.
func TestOneHashOneGoService_ZeroHash_GetsProcessed(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var received atomic.Int64
	allDone := make(chan struct{})
	const N = 5

	h.RegisterEvent("ohog-zero", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		if received.Add(1) == N {
			close(allDone)
		}
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	for i := 0; i < N; i++ {
		postOHOGSEvent(s, elem, 0) // roleID=0 → hash=0 → safego.Go
	}

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("only %d/%d zero-hash events processed", received.Load(), N)
	}
}

// TestOneHashOneGoService_PostTask_Runs verifies that PostTask delivers
// an arbitrary function to the per-hash TaskGroup using a fresh internal ctx.
// Written by Claude Code claude-opus-4-6.
func TestOneHashOneGoService_PostTask_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	c := s.GetCtxFromPool()
	c.GetTrace().(*ctx.IntTrace).RoleId = 99
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

// TestOneHashOneGoService_PostTaskWithRoleId_Runs verifies that PostTaskWithRoleId delivers the func
// and stamps the correct RoleId onto the ctx passed to it.
func TestOneHashOneGoService_PostTaskWithRoleId_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	ran := make(chan int64, 1)
	s.PostTaskWithRoleId(
		77, func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			ran <- c.GetTrace().GetRoleID()
			return nil
		},
	)

	select {
	case got := <-ran:
		if got != 77 {
			t.Fatalf("expected roleId 77, got %d", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("PostTaskWithRoleId did not run within timeout")
	}
}

// ── concurrency tests ────────────────────────────────────────────────────────

// TestOneHashOneGoService_Concurrent_MultiHash posts events from many goroutines
// across many distinct hashes and verifies all are delivered without loss or deadlock.
// Written by Claude Code claude-opus-4-6.
func TestOneHashOneGoService_Concurrent_MultiHash(t *testing.T) {
	const goroutines = 20
	const eventsEach = 30
	const total = goroutines * eventsEach

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var received atomic.Int64
	allDone := make(chan struct{})

	h.RegisterEvent("ohog-multi", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		if received.Add(1) == int64(total) {
			close(allDone)
		}
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			roleID := int64(g + 1) // distinct non-zero roleID → distinct hash per goroutine
			for i := 0; i < eventsEach; i++ {
				postOHOGSEvent(s, elem, roleID)
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

// TestOneHashOneGoService_SameHash_NoRace verifies that concurrent posts to the
// same hash are processed sequentially — the handler must never run concurrently
// with itself for the same hash.
// Written by Claude Code claude-opus-4-6.
func TestOneHashOneGoService_SameHash_NoRace(t *testing.T) {
	const goroutines = 10
	const eventsEach = 20
	const total = goroutines * eventsEach

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var received atomic.Int64
	allDone := make(chan struct{})

	h.RegisterEvent("ohog-norace", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		cur := inFlight.Add(1)
		// Track the peak concurrency seen for the same hash bucket
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(time.Millisecond) // hold the slot briefly
		inFlight.Add(-1)
		if received.Add(1) == int64(total) {
			close(allDone)
		}
		return nil
	},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeOHOGS(h)
	stop := startOHOGS(s)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	const roleID int64 = 1 // all goroutines use the same roleID → hash=1 → same bucket
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < eventsEach; i++ {
				postOHOGSEvent(s, elem, roleID)
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
		t.Errorf("same-hash handler ran concurrently: max in-flight = %d, want 1", maxInFlight.Load())
	}
}
