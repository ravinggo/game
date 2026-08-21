// Package handler_test tests the handler registration, middleware, and routing.
// Written by Claude Code claude-opus-4-6.
package handler

import (
	"testing"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
)

// newIntHandler returns a fresh Handler keyed on ctx.IntTrace for use in subtests.
func newIntHandler(mids ...Middleware[ctx.IntTrace, *ctx.IntTrace]) *Handler[ctx.IntTrace, *ctx.IntTrace] {
	return NewHandler[ctx.IntTrace, *ctx.IntTrace](mids...)
}

// newCtx returns a minimal BaseCtx suitable for exercising handlers in-process.
func newCtx() *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace] {
	c := &ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]{}
	c.Reset()
	return c
}

// --- 1. RegisterRPCResp basic registration and Lookup ---

func TestRegisterRPC_LookupFound(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCResp[*basepb.IntTrace, *basepb.ErrorMessage]("rpc-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
		false,
	)

	msgName := "basepb.IntTrace"
	elem, ok := h.Lookup(msgName)
	if !ok {
		t.Fatalf("Lookup(%q) returned false; expected registered elem", msgName)
	}
	if elem == nil {
		t.Fatal("Lookup returned nil elem")
	}
	if !elem.IsRPC() {
		t.Error("expected IsRPC() == true for RegisterRPCResp handler")
	}
	if !elem.IsRPCResp() {
		t.Error("expected IsRPCResp() == true for RegisterRPCResp handler")
	}
	if elem.IsForce() {
		t.Error("expected IsForce() == false for plain RegisterRPCResp")
	}
}

func TestRegisterRPC_LookupMiss(t *testing.T) {
	h := newIntHandler()
	_, ok := h.Lookup("basepb.NonExistent")
	if ok {
		t.Error("Lookup for unregistered message should return false")
	}
}

func TestRegisterRPC_MsgName(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCResp[*basepb.IntTrace, *basepb.ErrorMessage]("rpc-msgname",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
		false,
	)
	elem, _ := h.Lookup("basepb.IntTrace")
	if elem.MsgName() == "" {
		t.Error("MsgName() must not be empty after registration")
	}
}

// --- 2. RegisterRPCResp registration ---

func TestRegisterRPCResp_FlagsAndPools(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCResp[*basepb.ErrorMessage, *basepb.IntTrace]("rpcresp-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage, resp *basepb.IntTrace) *berror.ErrMsg {
			return nil
		},
		false,
	)

	elem, ok := h.Lookup("basepb.ErrorMessage")
	if !ok {
		t.Fatal("Lookup(basepb.ErrorMessage) should find RPCResp handler")
	}
	if !elem.IsRPC() {
		t.Error("RPCResp handler should have IsRPC() == true")
	}
	if !elem.IsRPCResp() {
		t.Error("RPCResp handler should have IsRPCResp() == true")
	}
	req, resp := elem.Acquire()
	if req == nil {
		t.Error("Acquire() req must not be nil for RPC handler")
	}
	if resp == nil {
		t.Error("Acquire() resp must not be nil for RPC handler")
	}
	elem.Release(req, resp)
}

// --- 3. RegisterEvent registration ---

func TestRegisterEvent_FlagsAndSubjMap(t *testing.T) {
	h := newIntHandler()
	h.RegisterEvent[*basepb.ErrorMessage]("event-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	elem, ok := h.Lookup("basepb.ErrorMessage")
	if !ok {
		t.Fatal("Lookup(basepb.ErrorMessage) should find event handler")
	}
	if elem.IsRPC() {
		t.Error("event handler should have IsRPC() == false")
	}
	if elem.IsForce() {
		t.Error("non-force event handler should have IsForce() == false")
	}
	req, resp := elem.Acquire()
	if resp != nil {
		t.Error("Acquire() resp must be nil for event handler")
	}
	elem.Release(req, resp)

	subjMap := h.GetQueueSubjInfo()
	if len(subjMap) == 0 {
		t.Error("queue subjMap should contain an entry after RegisterEvent")
	}
}

// --- 4. RegisterEventBroadcast registration ---

func TestRegisterEventBroadcast_BroadcastSubjMap(t *testing.T) {
	h := newIntHandler()
	h.RegisterEventBroadcast[*basepb.ErrorMessage]("broadcast-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	elem, ok := h.Lookup("basepb.ErrorMessage")
	if !ok {
		t.Fatal("Lookup should find broadcast handler")
	}
	if elem.IsRPC() {
		t.Error("broadcast handler should have IsRPC() == false")
	}

	broadcastMap := h.GetBroadcastSubjInfo()
	if len(broadcastMap) == 0 {
		t.Error("broadcastSubj should contain an entry after RegisterEventBroadcast")
	}
	queueMap := h.GetQueueSubjInfo()
	if len(queueMap) != 0 {
		t.Error("queue subjMap should remain empty when only broadcast is registered")
	}
}

// --- 5. RegisterEventForce flag check ---

func TestRegisterEventForce_IsForce(t *testing.T) {
	h := newIntHandler()
	h.RegisterEventForce[*basepb.ErrorMessage]("force-event",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	elem, ok := h.Lookup("basepb.ErrorMessage")
	if !ok {
		t.Fatal("Lookup should find force event handler")
	}
	if !elem.IsForce() {
		t.Error("force event handler should have IsForce() == true")
	}
}

func TestRegisterRPCForce_IsForce(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCRespForce[*basepb.IntTrace, *basepb.ErrorMessage]("force-rpc",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	elem, ok := h.Lookup("basepb.IntTrace")
	if !ok {
		t.Fatal("Lookup should find force RPC handler")
	}
	if !elem.IsForce() {
		t.Error("RegisterRPCRespForce handler should have IsForce() == true")
	}
}

// --- 6. Duplicate registration panics ---

func TestDuplicateRegistration_Panics(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCResp[*basepb.IntTrace, *basepb.ErrorMessage]("first",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
		false,
	)

	defer func() {
		if r := recover(); r == nil {
			t.Error("registering the same proto message type twice should panic")
		}
	}()

	h.RegisterRPCResp[*basepb.IntTrace, *basepb.ErrorMessage]("duplicate",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
		false,
	)
}

func TestDuplicateEventRegistration_Panics(t *testing.T) {
	h := newIntHandler()
	h.RegisterEvent[*basepb.ErrorMessage]("first-event",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	defer func() {
		if r := recover(); r == nil {
			t.Error("registering the same event message type twice should panic")
		}
	}()

	h.RegisterEvent[*basepb.ErrorMessage]("duplicate-event",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)
}

// --- 7. Subject conflict (broadcast vs non-broadcast) panics ---

func TestSubjectConflict_BroadcastThenNormal_Panics(t *testing.T) {
	// Register ErrorMessage as broadcast first, then try to register a normal handler
	// for a different message in the same "basepb." package namespace — this triggers
	// the panic because the subject prefix "basepb." would collide.
	//
	// The two messages in the test are in the same basepb package so they share
	// the "basepb." subject prefix; after the broadcast registration the normal
	// registration of any basepb message should panic.
	h := newIntHandler()
	h.RegisterEventBroadcast[*basepb.ErrorMessage]("broadcast",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	defer func() {
		if r := recover(); r == nil {
			t.Error("registering a normal handler under a broadcast subject prefix should panic")
		}
	}()

	h.RegisterEvent[*basepb.IntTrace]("normal-after-broadcast",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			return nil
		},
	)
}

func TestSubjectConflict_NormalThenBroadcast_Panics(t *testing.T) {
	h := newIntHandler()
	h.RegisterEvent[*basepb.IntTrace]("normal",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			return nil
		},
	)

	defer func() {
		if r := recover(); r == nil {
			t.Error("registering a broadcast handler under a normal subject prefix should panic")
		}
	}()

	h.RegisterEventBroadcast[*basepb.ErrorMessage]("broadcast-after-normal",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)
}

// --- 8. Group() shares handle map, adds middlewares ---

func TestGroup_SharesHandleMap(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCResp[*basepb.IntTrace, *basepb.ErrorMessage]("shared",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
		false,
	)

	g := h.Group()
	elem, ok := g.Lookup("basepb.IntTrace")
	if !ok {
		t.Fatal("Group should share the parent's routing table; Lookup should succeed")
	}
	if elem == nil {
		t.Fatal("Lookup via Group returned nil elem")
	}
}

func TestGroup_RegisterInGroupVisibleInParent(t *testing.T) {
	h := newIntHandler()
	g := h.Group()

	g.RegisterEvent[*basepb.ErrorMessage](
		"group-event",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	// Registration made through the group must appear in the parent's table
	// because they share the same underlying map.
	_, ok := h.Lookup("basepb.ErrorMessage")
	if !ok {
		t.Error("handler registered via Group should be visible through the parent Handler")
	}
}

func TestGroup_AddsMiddlewares(t *testing.T) {
	var callOrder []string

	outerMid := func(next HandleFunc[ctx.IntTrace, *ctx.IntTrace]) HandleFunc[ctx.IntTrace, *ctx.IntTrace] {
		return func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			callOrder = append(callOrder, "outer")
			return next(c)
		}
	}
	innerMid := func(next HandleFunc[ctx.IntTrace, *ctx.IntTrace]) HandleFunc[ctx.IntTrace, *ctx.IntTrace] {
		return func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			callOrder = append(callOrder, "inner")
			return next(c)
		}
	}

	h := newIntHandler(outerMid)
	g := h.Group(innerMid)

	g.RegisterEvent[*basepb.ErrorMessage](
		"group-mid",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			callOrder = append(callOrder, "handler")
			return nil
		},
	)

	elem, ok := g.Lookup("basepb.ErrorMessage")
	if !ok {
		t.Fatal("handler registered in group not found via Lookup")
	}

	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	elem.Call(c)

	if len(callOrder) != 3 {
		t.Fatalf("expected 3 call-order entries (outer, inner, handler), got %v", callOrder)
	}
	if callOrder[0] != "outer" || callOrder[1] != "inner" || callOrder[2] != "handler" {
		t.Errorf("unexpected middleware execution order: %v", callOrder)
	}
}

// --- 9. Elem.Call() with middleware chain ---

func TestElemCall_MiddlewareChainOrder(t *testing.T) {
	var trace []string

	mid1 := func(next HandleFunc[ctx.IntTrace, *ctx.IntTrace]) HandleFunc[ctx.IntTrace, *ctx.IntTrace] {
		return func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			trace = append(trace, "mid1-before")
			err := next(c)
			trace = append(trace, "mid1-after")
			return err
		}
	}
	mid2 := func(next HandleFunc[ctx.IntTrace, *ctx.IntTrace]) HandleFunc[ctx.IntTrace, *ctx.IntTrace] {
		return func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			trace = append(trace, "mid2-before")
			err := next(c)
			trace = append(trace, "mid2-after")
			return err
		}
	}

	h := newIntHandler(mid1, mid2)
	h.RegisterEvent[*basepb.ErrorMessage]("chain-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			trace = append(trace, "handler")
			return nil
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	elem.Call(c)

	want := []string{"mid1-before", "mid2-before", "handler", "mid2-after", "mid1-after"}
	if len(trace) != len(want) {
		t.Fatalf("expected %d trace entries, got %d: %v", len(want), len(trace), trace)
	}
	for i, v := range want {
		if trace[i] != v {
			t.Errorf("trace[%d] = %q, want %q", i, trace[i], v)
		}
	}
}

func TestElemCall_ReturnsHandlerError(t *testing.T) {
	h := newIntHandler()
	h.RegisterEvent[*basepb.ErrorMessage]("error-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return berror.NewProtocolStr("test error")
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	err := elem.Call(c)
	if err == nil {
		t.Error("expected non-nil error from handler")
	}
}

func TestElemCall_NoMiddlewareRunsHandlerDirectly(t *testing.T) {
	invoked := false
	h := newIntHandler()
	h.RegisterEvent[*basepb.ErrorMessage]("no-mid",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			invoked = true
			return nil
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	elem.Call(c)

	if !invoked {
		t.Error("handler should have been invoked with no middleware")
	}
}

// --- 10. Logger middleware ---

func TestLogger_SuccessPath(t *testing.T) {
	h := newIntHandler(Logger[ctx.IntTrace, *ctx.IntTrace])
	h.RegisterEvent[*basepb.ErrorMessage]("success-logger",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	err := elem.Call(c)
	if err != nil {
		t.Errorf("Logger success path should return nil error, got: %v", err)
	}
}

// --- 11. Recover middleware catches panics ---

func TestRecover_CatchesPanic(t *testing.T) {
	h := newIntHandler(Recover[ctx.IntTrace, *ctx.IntTrace])
	h.RegisterEvent[*basepb.ErrorMessage]("recover-panic",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			panic("recover test")
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}

	var err *berror.ErrMsg
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Recover middleware should have caught panic, but it propagated: %v", r)
			}
		}()
		err = elem.Call(c)
	}()

	if err == nil {
		t.Error("Recover middleware should convert panic to non-nil ErrMsg")
	}
}

func TestRecover_PassthroughOnSuccess(t *testing.T) {
	h := newIntHandler(Recover[ctx.IntTrace, *ctx.IntTrace])
	h.RegisterEvent[*basepb.ErrorMessage]("recover-ok",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	err := elem.Call(c)
	if err != nil {
		t.Errorf("Recover passthrough should not affect successful handler, got: %v", err)
	}
}

func TestRecover_PassthroughHandlerError(t *testing.T) {
	h := newIntHandler(Recover[ctx.IntTrace, *ctx.IntTrace])
	h.RegisterEvent[*basepb.ErrorMessage]("recover-err",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.ErrorMessage) *berror.ErrMsg {
			return berror.NewProtocolStr("planned error")
		},
	)

	elem, _ := h.Lookup("basepb.ErrorMessage")
	c := newCtx()
	c.Req = &basepb.ErrorMessage{}
	err := elem.Call(c)
	if err == nil {
		t.Error("Recover should pass through non-panic errors unchanged")
	}
}

// --- 12. GetHandler() self-access on *Handler ---

func TestGetHandler_ReturnsSelf(t *testing.T) {
	h := newIntHandler()
	got := h.GetHandler()
	if got != h {
		t.Error("GetHandler() should return the receiver itself")
	}
}

// --- Additional: Elem.String() and pool accessors coverage ---

func TestElemString_NotEmpty(t *testing.T) {
	h := newIntHandler()
	h.RegisterRPCResp[*basepb.IntTrace, *basepb.ErrorMessage]("string-test",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace, resp *basepb.ErrorMessage) *berror.ErrMsg {
			return nil
		},
		false,
	)
	elem, _ := h.Lookup("basepb.IntTrace")
	if elem.String() == "" {
		t.Error("Elem.String() should not return empty string")
	}
}

func TestGetQueueAndBroadcastSubjInfo(t *testing.T) {
	h := newIntHandler()
	h.RegisterEvent[*basepb.IntTrace]("subj-info",
		func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], req *basepb.IntTrace) *berror.ErrMsg {
			return nil
		},
	)

	q := h.GetQueueSubjInfo()
	b := h.GetBroadcastSubjInfo()
	if len(q) == 0 {
		t.Error("GetQueueSubjInfo should be non-empty after RegisterEvent")
	}
	if len(b) != 0 {
		t.Error("GetBroadcastSubjInfo should be empty when no broadcast handlers registered")
	}
}
