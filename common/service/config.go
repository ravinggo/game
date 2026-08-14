package service

import (
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/natsclient"
)

// HookUserMsg is an optional callback invoked for every inbound per-user NATS message
// when registered via ServerUserHookUserMsgOption. It receives the parsed user subject,
// raw trace bytes, raw proto payload bytes, and the original NATS message.
// Written by Claude Code claude-opus-4-6.
type HookUserMsg[T1 any, US natsclient.ServerUserSubjectPtr[T1]] = func(us US, traceData []byte, data []byte, msg *nats.Msg) bool

// middlewareScope controls which execution paths a middleware is applied to.
type middlewareScope uint8

const (
	scopeHandler middlewareScope = iota // NATS messages only
	scopeService                        // NATS messages + PostTaskXXX
)

// scopedMiddle pairs a middleware with its execution scope.
type scopedMiddle[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	mid   handler.Middleware[TraceData, TP]
	scope middlewareScope
}

// config holds construction-time settings shared by all service types. It is populated
// by applying functional Option values before the BaseService is created.
// Written by Claude Code claude-opus-4-6.
type config[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	lockQueueThread    bool
	disableLogger      bool
	disableRecover     bool
	respFirst          bool
	rpcTimeout         time.Duration
	idleCleanupTimeout time.Duration
	middles            []scopedMiddle[TraceData, TP]
	natsOptions        []nats.Option
	initCtx            func(*ctx.BaseCtx[TraceData, TP])
}

// allMiddlewares returns the full middleware chain for NATS message handling.
// Logger and Recover are prepended by default (outermost first: Logger → Recover → user middlewares).
// Either can be suppressed via DisableLoggerOption / DisableRecoverOption.
func (c *config[TraceData, TP]) allMiddlewares() []handler.Middleware[TraceData, TP] {
	all := make([]handler.Middleware[TraceData, TP], 0, len(c.middles)+2)
	if !c.disableLogger {
		all = append(all, handler.Logger[TraceData, TP])
	}
	if !c.disableRecover {
		all = append(all, handler.Recover[TraceData, TP])
	}
	for _, sm := range c.middles {
		all = append(all, sm.mid)
	}
	return all
}

// serviceMiddlewares returns only the service-scoped middlewares in declaration order.
// Stored on BaseService and applied to PostTaskXXX execution.
func (c *config[TraceData, TP]) serviceMiddlewares() []handler.Middleware[TraceData, TP] {
	var svc []handler.Middleware[TraceData, TP]
	for _, sm := range c.middles {
		if sm.scope == scopeService {
			svc = append(svc, sm.mid)
		}
	}
	return svc
}

// Option is a functional option that mutates a config during service construction.
// Written by Claude Code claude-opus-4-6.
type Option[TraceData any, TP ctx.TracePtr[TraceData]] func(*config[TraceData, TP])

// buildConfig applies a slice of Option values to a zero-value config and returns
// the resulting configuration. Called by every concrete service constructor.
// Written by Claude Code claude-opus-4-6.
func buildConfig[TraceData any, TP ctx.TracePtr[TraceData]](ops []Option[TraceData, TP]) config[TraceData, TP] {
	c := config[TraceData, TP]{}
	for _, op := range ops {
		op(&c)
	}
	return c
}

// NatsOptions returns an Option that appends the given nats.Option values to the
// underlying NATS client configuration.
// Written by Claude Code claude-opus-4-6.
func NatsOptions[TraceData any, TP ctx.TracePtr[TraceData]](
	opts ...nats.Option,
) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.natsOptions = append(c.natsOptions, opts...)
	}
}

// LockQueueThreadOption returns an Option that locks the EventLoop's dequeue goroutine
// to its OS thread, which can reduce latency jitter on latency-sensitive services.
// Written by Claude Code claude-opus-4-6.
func LockQueueThreadOption[TraceData any, TP ctx.TracePtr[TraceData]]() Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.lockQueueThread = true
	}
}

// RPCTimeoutOption returns an Option that sets the NATS RPC reply deadline.
// The default is 10 seconds when no timeout is specified.
// Written by Claude Code claude-opus-4-6.
func RPCTimeoutOption[TraceData any, TP ctx.TracePtr[TraceData]](
	timeout time.Duration,
) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.rpcTimeout = timeout
	}
}

// IdleCleanupTimeoutOption sets the inactivity duration after which a per-RoleID goroutine
// in OneHashOneGoService is evicted and returned to the pool. Defaults to 30 seconds.
func IdleCleanupTimeoutOption[TraceData any, TP ctx.TracePtr[TraceData]](
	timeout time.Duration,
) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.idleCleanupTimeout = timeout
	}
}

// HandlerMiddleOption appends handler-scoped middlewares that apply only to NATS messages.
func HandlerMiddleOption[TraceData any, TP ctx.TracePtr[TraceData]](
	middles ...handler.Middleware[TraceData, TP],
) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		for _, m := range middles {
			c.middles = append(c.middles, scopedMiddle[TraceData, TP]{mid: m, scope: scopeHandler})
		}
	}
}

// ServiceMiddleOption appends service-scoped middlewares that apply to both NATS messages
// and PostTaskXXX calls. Declaration order relative to HandlerMiddleOption entries is preserved
// for the NATS path; only service-scoped entries execute on the PostTask path.
func ServiceMiddleOption[TraceData any, TP ctx.TracePtr[TraceData]](
	middles ...handler.Middleware[TraceData, TP],
) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		for _, m := range middles {
			c.middles = append(c.middles, scopedMiddle[TraceData, TP]{mid: m, scope: scopeService})
		}
	}
}

// DisableLoggerOption disables the default Logger middleware for this service.
func DisableLoggerOption[TraceData any, TP ctx.TracePtr[TraceData]]() Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) { c.disableLogger = true }
}

// DisableRecoverOption disables the default Recover middleware for this service.
func DisableRecoverOption[TraceData any, TP ctx.TracePtr[TraceData]]() Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) { c.disableRecover = true }
}

// RespFirstOption makes the RPC Resp message appear before OtherResp messages in the reply.
// By default Resp is placed after OtherResp (isRespFirst == false).
func RespFirstOption[TraceData any, TP ctx.TracePtr[TraceData]]() Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) { c.respFirst = true }
}

// MiddlesOption is an alias for HandlerMiddleOption for backward compatibility.
func MiddlesOption[TraceData any, TP ctx.TracePtr[TraceData]](
	middles ...handler.Middleware[TraceData, TP],
) Option[TraceData, TP] {
	return HandlerMiddleOption[TraceData, TP](middles...)
}

// InitCtxOption returns an Option that registers a hook called every time a BaseCtx
// is acquired from the pool, allowing callers to set default fields on each context.
func InitCtxOption[TraceData any, TP ctx.TracePtr[TraceData]](
	f func(*ctx.BaseCtx[TraceData, TP]),
) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.initCtx = f
	}
}

// ServerUserOption is a functional option for configuring a ServerUserService
// during construction.
// Written by Claude Code claude-opus-4-6.
type ServerUserOption[
T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1],
] func(*serverUserConfig[T1, TraceData, TP, US])

// serverUserConfig accumulates settings for ServerUserService construction, including
// the base service options, an optional message hook, and per-user NATS options.
// Written by Claude Code claude-opus-4-6.
type serverUserConfig[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] struct {
	options     []Option[TraceData, TP]
	hookUserMsg HookUserMsg[T1, US]
	unOptions   []natsclient.UNOption
}

// ServerUserBase returns a ServerUserOption that forwards base-service Option values to
// the underlying OneHashOneGoService, allowing shared settings such as middleware and
// RPC timeout to be configured through the ServerUserService constructor.
// Written by Claude Code claude-opus-4-6.
func ServerUserBase[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](
	options ...Option[TraceData, TP],
) ServerUserOption[T1, TraceData, TP, US] {
	return func(s *serverUserConfig[T1, TraceData, TP, US]) {
		s.options = append(s.options, options...)
	}
}

// ServerUserHookUserMsgOption returns a ServerUserOption that registers a HookUserMsg
// callback. When set, the hook intercepts every inbound per-user message instead of
// the default proto dispatch logic.
// Written by Claude Code claude-opus-4-6.
func ServerUserHookUserMsgOption[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](
	hook HookUserMsg[T1, US],
) ServerUserOption[T1, TraceData, TP, US] {
	return func(s *serverUserConfig[T1, TraceData, TP, US]) {
		s.hookUserMsg = hook
	}
}

// ServerUserUNOption returns a ServerUserOption that appends natsclient.UNOption values
// to the per-user NATS cluster client, for example to configure reconnect behaviour
// or TLS settings specific to user subscriptions.
// Written by Claude Code claude-opus-4-6.
func ServerUserUNOption[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](
	options ...natsclient.UNOption,
) ServerUserOption[T1, TraceData, TP, US] {
	return func(s *serverUserConfig[T1, TraceData, TP, US]) {
		s.unOptions = append(s.unOptions, options...)
	}
}
