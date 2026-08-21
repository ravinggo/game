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

// Config holds construction-time settings shared by all service types. It is populated
// by applying functional Option values before the BaseService is created.
// Written by Claude Code claude-opus-4-6.
type Config[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	LockQueueThread    bool
	DisableLogger      bool
	DisableRecover     bool
	RespFirst          bool
	RpcTimeout         time.Duration
	IdleCleanupTimeout time.Duration
	Middles            []scopedMiddle[TraceData, TP]
	NatsOptions        []nats.Option
	InitCtx            func(*ctx.BaseCtx[TraceData, TP])
	DispatchHook       func(msg *nats.Msg) bool
}

// allMiddlewares returns the full middleware chain for NATS message handling.
// Logger and Recover are prepended by default (outermost first: Logger → Recover → user middlewares).
// Either can be suppressed via DisableLoggerOption / DisableRecoverOption.
func (c *Config[TraceData, TP]) allMiddlewares() []handler.Middleware[TraceData, TP] {
	all := make([]handler.Middleware[TraceData, TP], 0, len(c.Middles)+2)
	if !c.DisableLogger {
		all = append(all, handler.Logger[TraceData, TP])
	}
	if !c.DisableRecover {
		all = append(all, handler.Recover[TraceData, TP])
	}
	for _, sm := range c.Middles {
		all = append(all, sm.mid)
	}
	return all
}

// serviceMiddlewares returns only the service-scoped middlewares in declaration order.
// Stored on BaseService and applied to PostTaskXXX execution.
func (c *Config[TraceData, TP]) serviceMiddlewares() []handler.Middleware[TraceData, TP] {
	var svc []handler.Middleware[TraceData, TP]
	for _, sm := range c.Middles {
		if sm.scope == scopeService {
			svc = append(svc, sm.mid)
		}
	}
	return svc
}

// ServerUserOption is a functional option for configuring a ServerUserService
// during construction.
// Written by Claude Code claude-opus-4-6.
type ServerUserOption[
T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1],
] func(*ServerUserConfig[T1, TraceData, TP, US])

// ServerUserConfig accumulates settings for ServerUserService construction, including
// the base service Options, an optional message hook, and per-user NATS Options.
// Written by Claude Code claude-opus-4-6.
type ServerUserConfig[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] struct {
	Config[TraceData, TP]
	HookUserMsg HookUserMsg[T1, US]
	UNOptions   []natsclient.UNOption
}
