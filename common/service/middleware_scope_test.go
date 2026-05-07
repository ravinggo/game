package service

import (
	"testing"
	"time"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
)

// makeOrderMiddleware returns a middleware that appends name to calls when entered.
func makeOrderMiddleware(name string, calls *[]string) handler.Middleware[ctx.IntTrace, *ctx.IntTrace] {
	return func(next handler.HandleFunc[ctx.IntTrace, *ctx.IntTrace]) handler.HandleFunc[ctx.IntTrace, *ctx.IntTrace] {
		return func(c *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
			*calls = append(*calls, name)
			return next(c)
		}
	}
}

// TestMiddlewareScope_NATSPath_AllMiddlewaresRun verifies that both service-scoped and
// handler-scoped middlewares execute on the NATS message path, in declaration order.
// Simulates: ServiceMiddleOption(A), HandlerMiddleOption(B), ServiceMiddleOption(C).
// Expected NATS chain: A → B → C → handler.
func TestMiddlewareScope_NATSPath_AllMiddlewaresRun(t *testing.T) {
	var calls []string
	done := make(chan struct{})

	// All middlewares go into the handler (simulating allMiddlewares())
	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace](
		makeOrderMiddleware("A", &calls),
		makeOrderMiddleware("B", &calls),
		makeOrderMiddleware("C", &calls),
	)
	handler.RegisterEvent(h, "scope-nats", func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace], _ *basepb.IntTrace) *berror.ErrMsg {
		calls = append(calls, "handler")
		close(done)
		return nil
	})
	elem, _ := h.Lookup("basepb.IntTrace")

	s := makeELS(h)
	stop := startELS(s)
	defer stop()

	postELSEvent(s, elem, 1)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called within timeout")
	}

	want := []string{"A", "B", "C", "handler"}
	if len(calls) != len(want) {
		t.Fatalf("NATS path: got %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("NATS path order at [%d]: got %q want %q (full: %v)", i, calls[i], want[i], calls)
		}
	}
}

// TestMiddlewareScope_PostTask_HandlerScopeSkipped verifies that handler-scoped middlewares
// do NOT run on the PostTask path, while service-scoped ones do in their relative order.
// Simulates: ServiceMiddleOption(A), HandlerMiddleOption(B), ServiceMiddleOption(C).
// Expected PostTask chain: A → C → f  (B absent).
func TestMiddlewareScope_PostTask_HandlerScopeSkipped(t *testing.T) {
	var calls []string
	done := make(chan struct{})

	h := handler.NewHandler[ctx.IntTrace, *ctx.IntTrace]()
	s := makeELS(h)
	// serviceMiddles = [A, C] — only service-scoped middlewares, relative order preserved
	s.BaseService.serviceMiddles = []handler.Middleware[ctx.IntTrace, *ctx.IntTrace]{
		makeOrderMiddleware("A", &calls),
		makeOrderMiddleware("C", &calls),
	}

	stop := startELS(s)
	defer stop()

	s.PostTaskWithRoleId(1, func(_ *ctx.BaseCtx[ctx.IntTrace, *ctx.IntTrace]) *berror.ErrMsg {
		calls = append(calls, "f")
		close(done)
		return nil
	})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("PostTaskWithRoleId did not run within timeout")
	}

	want := []string{"A", "C", "f"}
	if len(calls) != len(want) {
		t.Fatalf("PostTask path: got %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("PostTask path order at [%d]: got %q want %q (full: %v)", i, calls[i], want[i], calls)
		}
	}
}
