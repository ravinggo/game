// Tests for FixedFrameAbsService / FixedFrameTickerService — unit and concurrency.
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

// makeFFAbs constructs a FixedFrameAbsService without a NATS connection.
func makeFFAbs(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace], fps int) *FixedFrameAbsService[ctx.IntTrace, *ctx.IntTrace] {
	loop := eventloop.NewAbsFrameLoop(fps, true)
	s := &FixedFrameAbsService[ctx.IntTrace, *ctx.IntTrace]{
		fixedFrameService: fixedFrameService[ctx.IntTrace, *ctx.IntTrace]{
			BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
				ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
				h:       h,
			},
			fl: loop,
		},
		loop: loop,
	}
	s.dispatch = func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
		loop.PostEventQueue(ce[ctx.IntTrace, *ctx.IntTrace]{Ctx: c, Elem: elem})
	}
	return s
}

// makeFFTicker constructs a FixedFrameTickerService without a NATS connection.
func makeFFTicker(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace], fps int) *FixedFrameTickerService[ctx.IntTrace, *ctx.IntTrace] {
	loop := eventloop.NewTickerFrameLoop(fps, false)
	s := &FixedFrameTickerService[ctx.IntTrace, *ctx.IntTrace]{
		fixedFrameService: fixedFrameService[ctx.IntTrace, *ctx.IntTrace]{
			BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
				ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
				h:       h,
			},
			fl: loop,
		},
		loop: loop,
	}
	s.dispatch = func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
		loop.PostEventQueue(ce[ctx.IntTrace, *ctx.IntTrace]{Ctx: c, Elem: elem})
	}
	return s
}

// makeFFCatchUp constructs a FixedFrameCatchUpService without a NATS connection.
func makeFFCatchUp(h *handler.Handler[ctx.IntTrace, *ctx.IntTrace], fps int) *FixedFrameCatchUpService[ctx.IntTrace, *ctx.IntTrace] {
	loop := eventloop.NewCatchUpFrameLoop(fps, false)
	s := &FixedFrameCatchUpService[ctx.IntTrace, *ctx.IntTrace]{
		fixedFrameService: fixedFrameService[ctx.IntTrace, *ctx.IntTrace]{
			BaseService: &BaseService[ctx.IntTrace, *ctx.IntTrace]{
				ctxPool: objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
				h:       h,
			},
			fl: loop,
		},
		loop: loop,
	}
	s.dispatch = func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
		loop.PostEventQueue(ce[ctx.IntTrace, *ctx.IntTrace]{Ctx: c, Elem: elem})
	}
	return s
}

// startFFS starts the frame loop of a fixed-frame service bypassing NATS
// subscription, and returns a stop func.
func startFFS(s *fixedFrameService[ctx.IntTrace, *ctx.IntTrace], frame eventloop.FrameFunc) func() {
	s.fl.Start(frame, s.handleEvent(func(any) {}))
	return s.fl.Stop
}

// postFFSEvent dispatches one event with the given RoleId.
func postFFSEvent(s *fixedFrameService[ctx.IntTrace, *ctx.IntTrace], elem *handler.Elem[ctx.IntTrace, *ctx.IntTrace], roleID int64) {
	c := s.GetCtxFromPool()
	r, _ := elem.Acquire()
	r.(*basepb.IntTrace).RoleId = roleID
	c.Req = r
	s.dispatch(c, elem)
}

// ── unit tests ──────────────────────────────────────────────────────────────

// TestFixedFrameAbsService_DispatchCallsHandler verifies a dispatched event
// reaches the registered handler on the abs frame loop.
func TestFixedFrameAbsService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	var once sync.Once
	handler.RegisterEvent(
		h, "ffs-abs-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			once.Do(func() { close(done) })
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFFAbs(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	postFFSEvent(&s.fixedFrameService, elem, 1)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not called within timeout")
	}
}

// TestFixedFrameTickerService_DispatchCallsHandler verifies a dispatched event
// reaches the registered handler on the ticker frame loop.
func TestFixedFrameTickerService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	var once sync.Once
	handler.RegisterEvent(
		h, "ffs-ticker-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			once.Do(func() { close(done) })
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFFTicker(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	postFFSEvent(&s.fixedFrameService, elem, 1)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not called within timeout")
	}
}

// TestFixedFrameService_Sequential_Order verifies that events posted from one
// goroutine are processed in arrival order at frame boundaries.
func TestFixedFrameService_Sequential_Order(t *testing.T) {
	const N = 30
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	results := make([]int64, 0, N)
	var mu sync.Mutex
	allDone := make(chan struct{})
	var count int

	handler.RegisterEvent(
		h, "ffs-order", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
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

	s := makeFFAbs(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	for i := 0; i < N; i++ {
		postFFSEvent(&s.fixedFrameService, elem, int64(i))
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

// TestFixedFrameService_FrameCallbackRuns verifies the per-frame callback is
// invoked with increasing frame counts.
func TestFixedFrameService_FrameCallbackRuns(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFFAbs(h, 100)

	frames := make(chan int64, 8)
	stop := startFFS(
		&s.fixedFrameService, func(frameCount int64, delta time.Duration) {
			if delta != s.FrameInterval() {
				t.Errorf("delta = %v, want %v", delta, s.FrameInterval())
			}
			select {
			case frames <- frameCount:
			default:
			}
		},
	)
	defer stop()

	var prev int64
	for i := 0; i < 3; i++ {
		select {
		case fc := <-frames:
			if fc <= prev {
				t.Fatalf("frameCount not increasing: prev=%d got=%d", prev, fc)
			}
			prev = fc
		case <-time.After(3 * time.Second):
			t.Fatal("frame callback did not run within timeout")
		}
	}
}

// TestFixedFrameService_PostTaskWithRoleId_Runs verifies PostTaskWithRoleId
// delivers the func with the correct RoleId on the frame goroutine.
func TestFixedFrameService_PostTaskWithRoleId_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFFTicker(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
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

// TestFixedFrameService_PostFunc_Runs verifies PostFunc executes the func on
// the frame goroutine.
func TestFixedFrameService_PostFunc_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFFAbs(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	ran := make(chan struct{})
	s.PostFunc(func() { close(ran) })

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("PostFunc did not run within timeout")
	}
}

// ── concurrency tests ────────────────────────────────────────────────────────

// TestFixedFrameService_ConcurrentPost_AllDelivered posts events from many
// goroutines and verifies lossless delivery through frame batches.
func TestFixedFrameService_ConcurrentPost_AllDelivered(t *testing.T) {
	const goroutines = 20
	const eventsPerGoroutine = 50
	const total = goroutines * eventsPerGoroutine

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var received atomic.Int64
	allDone := make(chan struct{})

	handler.RegisterEvent(
		h, "ffs-concurrent", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			if received.Add(1) == int64(total) {
				close(allDone)
			}
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFFAbs(h, 200)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < eventsPerGoroutine; i++ {
				postFFSEvent(&s.fixedFrameService, elem, int64(g*eventsPerGoroutine+i))
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

// TestFixedFrameService_StopDrainsEvents verifies queued events drain on Stop.
func TestFixedFrameService_StopDrainsEvents(t *testing.T) {
	const N = 10
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var count atomic.Int64

	handler.RegisterEvent(
		h, "ffs-stop", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			count.Add(1)
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFFTicker(h, 1) // 1fps: events below drain on Stop, not on a tick
	stop := startFFS(&s.fixedFrameService, nil)

	for i := 0; i < N; i++ {
		postFFSEvent(&s.fixedFrameService, elem, int64(i))
	}
	stop() // blocks until loop exits

	if got := count.Load(); got != N {
		t.Fatalf("drained %d events on Stop, want %d", got, N)
	}
}

// TestFixedFrameCatchUpService_DispatchCallsHandler verifies a dispatched
// event reaches the registered handler on the catch-up frame loop.
func TestFixedFrameCatchUpService_DispatchCallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	done := make(chan struct{})
	var once sync.Once
	handler.RegisterEvent(
		h, "ffs-catchup-basic", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
			once.Do(func() { close(done) })
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeFFCatchUp(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	postFFSEvent(&s.fixedFrameService, elem, 1)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler was not called within timeout")
	}
}

// TestFixedFrameCatchUpService_FrameCallbackContiguous verifies frame counts
// delivered by the catch-up service are contiguous and delta is the fixed
// interval.
func TestFixedFrameCatchUpService_FrameCallbackContiguous(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFFCatchUp(h, 100)

	var mu sync.Mutex
	var counts []int64
	var once sync.Once
	enough := make(chan struct{})
	stop := startFFS(
		&s.fixedFrameService, func(frameCount int64, delta time.Duration) {
			if delta != s.FrameInterval() {
				t.Errorf("delta = %v, want %v", delta, s.FrameInterval())
			}
			mu.Lock()
			counts = append(counts, frameCount)
			n := len(counts)
			mu.Unlock()
			if n >= 5 {
				once.Do(func() { close(enough) })
			}
		},
	)
	defer stop()

	select {
	case <-enough:
	case <-time.After(3 * time.Second):
		t.Fatal("frame callback did not run 5 frames within timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	for i, v := range counts[:5] {
		if v != int64(i+1) {
			t.Fatalf("frame counts must be contiguous: index %d has frameCount %d, want %d", i, v, i+1)
		}
	}
}

// TestFixedFrameCatchUpService_PostFunc_Runs verifies PostFunc executes on the
// catch-up frame goroutine.
func TestFixedFrameCatchUpService_PostFunc_Runs(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeFFCatchUp(h, 100)
	stop := startFFS(&s.fixedFrameService, nil)
	defer stop()

	ran := make(chan struct{})
	s.PostFunc(func() { close(ran) })

	select {
	case <-ran:
	case <-time.After(3 * time.Second):
		t.Fatal("PostFunc did not run within timeout")
	}
}
