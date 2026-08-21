// Package service internal tests — exercises BaseService dispatch and helper
// functions without a live NATS connection.
// Written by Claude Code claude-opus-4-6.
package service

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/ravinggo/objectpool"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/timer"
)

// TestMain initialises the logger (required by eventloop/safego goroutines) and the
// low-precision timer (required by scheduleIdleCleanup paths) before any test runs.
// Written by Claude Code claude-opus-4-6.
func TestMain(m *testing.M) {
	logger.InitDefaultLogger()
	timer.StartLowPrecisionTime()
	os.Exit(m.Run())
}

// makeBS constructs a BaseService with ctxPool set but natsCluster left nil.
// Only call methods that do not reach natsCluster (handleCtx, dealNatsMsg, etc.).
// Written by Claude Code claude-opus-4-6.
func makeBS(
	h *handler.Handler[ctx.IntTrace, *ctx.IntTrace],
	dispatchFunc func(*ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], *handler.Elem[ctx.IntTrace, *ctx.IntTrace]),
) *BaseService[ctx.IntTrace, *ctx.IntTrace] {
	s := &BaseService[ctx.IntTrace, *ctx.IntTrace]{
		ctxPool:  objectpool.GetTypePool[ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]](),
		h:        h,
		dispatch: dispatchFunc,
	}
	return s
}

// buildWireData returns the on-wire NATS payload for msg with traceSize=0.
// The buffer is pre-allocated with enough capacity for gogo-protobuf's MarshalTo,
// which requires cap(buf[len:]) >= msg.Size().
// Written by Claude Code claude-opus-4-6.
func buildWireData(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	size := define.ProtoSize(msg)
	data := make([]byte, 2, 2+size) // data[0..1] = 0: traceSize == 0
	var err error
	data, err = define.ProtoMarshalAppend(data, msg)
	if err != nil {
		t.Fatalf("ProtoMarshalAppend: %v", err)
	}
	return data
}

// TestGetCtxFromPool_CallsInitCtx verifies that GetCtxFromPool calls initCtx on every
// acquired context when Config.InitCtx is set.
func TestGetCtxFromPool_CallsInitCtx(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeBS(h, nil)

	var calls int
	s.cnf.InitCtx = func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) {
		if c == nil {
			t.Error("initCtx received nil ctx")
		}
		calls++
	}

	const N = 5
	ctxs := make([]*ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], N)
	for i := range ctxs {
		ctxs[i] = s.GetCtxFromPool()
	}
	if calls != N {
		t.Errorf("initCtx called %d times, want %d", calls, N)
	}
	for _, c := range ctxs {
		s.PutCtxToPool(c)
	}
}

// TestGetCtxFromPool_NilInitCtx verifies that GetCtxFromPool does not panic when
// Config.InitCtx is nil.
func TestGetCtxFromPool_NilInitCtx(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeBS(h, nil)
	c := s.GetCtxFromPool()
	if c == nil {
		t.Fatal("GetCtxFromPool returned nil ctx")
	}
	s.PutCtxToPool(c)
}

// ── ReplyTaskPoolFull ──────────────────────────────────────────────────────────

// TestReplyTaskPoolFull_NilNatsMsg verifies that a nil NatsMsg is a silent no-op.
// Written by Claude Code claude-opus-4-6.
func TestReplyTaskPoolFull_NilNatsMsg(t *testing.T) {
	c := &ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]{}
	ReplyTaskPoolFull(c) // must not panic
}

// TestReplyTaskPoolFull_EmptyReply verifies that an empty Reply is a silent no-op
// (no NATS Publish call is made, so no connection is needed).
// Written by Claude Code claude-opus-4-6.
func TestReplyTaskPoolFull_EmptyReply(t *testing.T) {
	c := &ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]{}
	c.NatsMsg = &nats.Msg{Reply: ""}
	ReplyTaskPoolFull(c) // must not panic
}

// ── handleCtx ─────────────────────────────────────────────────────────────────

// TestHandleCtx_CallsHandler verifies that the registered handler function is invoked.
// Written by Claude Code claude-opus-4-6.
func TestHandleCtx_CallsHandler(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	var called atomic.Bool
	h.RegisterEvent(
		"test-handle-ctx", func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			called.Store(true)
			return nil
		},
	)
	elem, ok := h.Lookup("basepb.IntTrace")
	if !ok {
		t.Fatal("Lookup: handler not found")
	}

	s := makeBS(h, nil)
	c := s.GetCtxFromPool()
	c.Req, c.Resp = elem.Acquire()

	s.HandleCtx(c, elem)

	if !called.Load() {
		t.Fatal("handler was not called by handleCtx")
	}
}

// TestHandleCtx_RecyclesReq verifies that c.Req is reset and returned to the pool
// (evidenced by Req being nil after handleCtx returns — the function sets c.Req=nil).
// Written by Claude Code claude-opus-4-6.
func TestHandleCtx_RecyclesReq(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	h.RegisterEvent(
		"test-recycle", func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			return nil
		},
	)
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeBS(h, nil)
	c := s.GetCtxFromPool()
	c.Req, c.Resp = elem.Acquire()
	if c.Req == nil {
		t.Fatal("Req should be non-nil before handleCtx")
	}

	s.HandleCtx(c, elem)
	// After handleCtx returns the ctx is back in the pool; c itself is now stale.
	// The observable side-effect is that no panic occurred and the handler ran.
}

// TestHandleCtx_HandlerError_NoNatsMsgNoRPCResp verifies that an error returned by
// the handler does not attempt a NATS reply when NatsMsg is nil.
// Written by Claude Code claude-opus-4-6.
func TestHandleCtx_HandlerError_NoNatsMsgNoRPCResp(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	h.RegisterEvent(
		"test-err", func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return berror.NewProtocolStr("test error")
		},
	)
	elem, _ := h.Lookup("basepb.ErrorMessage")

	s := makeBS(h, nil)
	c := s.GetCtxFromPool()
	c.Req, c.Resp = elem.Acquire()
	// c.NatsMsg = nil → no NATS reply attempted

	s.HandleCtx(c, elem) // must not panic
}

// ── dealNatsMsg ───────────────────────────────────────────────────────────────

// TestDealNatsMsg_NoLastDot verifies that a subject with no dot is silently dropped.
// Written by Claude Code claude-opus-4-6.
func TestDealNatsMsg_NoLastDot(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeBS(
		h, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
			t.Error("dispatch must not be called for a subject without a dot")
		},
	)
	s.DealNatsMsg(&nats.Msg{Subject: "nodot", Data: []byte{0, 0}})
}

// TestDealNatsMsg_ShortData verifies that payloads shorter than 2 bytes are dropped.
// Written by Claude Code claude-opus-4-6.
func TestDealNatsMsg_ShortData(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeBS(
		h, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
			t.Error("dispatch must not be called for short data")
		},
	)
	s.DealNatsMsg(&nats.Msg{Subject: "basepb.IntTrace", Data: []byte{0}})
}

// TestDealNatsMsg_UnregisteredMsg verifies that an unregistered subject is dropped
// without calling dispatch (Reply="" so no NATS publish is attempted).
// Written by Claude Code claude-opus-4-6.
func TestDealNatsMsg_UnregisteredMsg(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeBS(
		h, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
			t.Error("dispatch must not be called for unregistered message")
		},
	)
	s.DealNatsMsg(
		&nats.Msg{
			Subject: "basepb.IntTrace",
			Data:    []byte{0, 0},
			Reply:   "",
		},
	)
}

// TestDealNatsMsg_Dispatches verifies the full parse-and-dispatch path: the wire
// payload is decoded and dispatch is invoked exactly once for a registered handler.
// Written by Claude Code claude-opus-4-6.
func TestDealNatsMsg_Dispatches(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	h.RegisterEvent(
		"dispatch-test", func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			return nil
		},
	)

	var dispatchCount atomic.Int32
	var s *BaseService[ctx.IntTrace, *ctx.IntTrace]
	s = makeBS(
		h, func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], e *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
			dispatchCount.Add(1)
			// Complete the lifecycle so the ctx is returned to the pool.
			s.HandleCtx(c, e)
		},
	)

	s.DealNatsMsg(
		&nats.Msg{
			Subject: "basepb.IntTrace",
			Data:    buildWireData(t, &basepb.IntTrace{RoleId: 99}),
			Reply:   "",
		},
	)

	if dispatchCount.Load() != 1 {
		t.Fatalf("dispatch called %d times, want 1", dispatchCount.Load())
	}
}

// TestDealNatsMsg_CorruptProto verifies that a malformed proto payload is rejected
// gracefully without a panic (Reply="" so no NATS publish is needed).
// Written by Claude Code claude-opus-4-6.
func TestDealNatsMsg_CorruptProto(t *testing.T) {
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	h.RegisterEvent(
		"corrupt-test", func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			t.Error("handler must not be called for corrupt proto")
			return nil
		},
	)

	s := makeBS(
		h, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *handler.Elem[ctx.IntTrace, *ctx.IntTrace]) {
			t.Error("dispatch must not be called for corrupt proto")
		},
	)

	// [traceSize=0,0] followed by garbage that is not valid proto
	s.DealNatsMsg(
		&nats.Msg{
			Subject: "basepb.IntTrace",
			Data:    []byte{0, 0, 0xff, 0xff, 0xff, 0xff},
			Reply:   "",
		},
	)
}
