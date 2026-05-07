package handler

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
)

// Middleware is a function that wraps a HandleFunc, enabling pre- and post-processing
// around the core handler — such as logging, tracing, authentication, or rate limiting.
// Middlewares are applied in registration order: the first middleware is the outermost wrapper.
// Written by Claude Code claude-opus-4-6.
type Middleware[TraceData any, TP ctx.TracePtr[TraceData]] func(HandleFunc[TraceData, TP]) HandleFunc[TraceData, TP]

// HandleFunc is the signature for all message handlers. It receives a typed context carrying
// the trace data, the request proto, and routing metadata, then returns a structured error or nil.
// Written by Claude Code claude-opus-4-6.
type HandleFunc[TraceData any, TP ctx.TracePtr[TraceData]] func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg

// IService is implemented by any type that owns a handler registry.
// All concrete service types in the service package implement this interface.
// *Handler also implements it so that handlers can be registered directly (e.g. in tests).
type IService[TraceData any, TP ctx.TracePtr[TraceData]] interface {
	GetHandler() *Handler[TraceData, TP]
}

// reqRespPair holds a pre-allocated request/response proto pair for pool storage.
// For event handlers resp is nil; for RPC handlers both fields are non-nil.
type reqRespPair struct {
	req  proto.Message
	resp proto.Message
}

// Elem holds all metadata and runtime state for a single registered message handler.
// It stores the composed handler function, the middleware chain, pool references for
// request/response proto messages, and classification flags (RPC, broadcast, force).
// Callers never construct Elem directly; it is produced by the Register* functions.
// Written by Claude Code claude-opus-4-6.
type Elem[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	h         HandleFunc[TraceData, TP]
	desc      string
	midS      []Middleware[TraceData, TP]
	isRPC     bool
	isRPCResp bool
	msgName   protoreflect.FullName
	funcType  string
	isForce bool
	msgPool *sync.Pool
}

// IsForce reports whether the handler bypasses backpressure checks.
func (this_ *Elem[TraceData, TP]) IsForce() bool   { return this_.isForce }

// IsRPC reports whether the handler expects a reply to be sent back to the caller.
func (this_ *Elem[TraceData, TP]) IsRPC() bool     { return this_.isRPC }

// IsRPCResp reports whether the response proto is pooled by the framework.
func (this_ *Elem[TraceData, TP]) IsRPCResp() bool { return this_.isRPCResp }

// Acquire retrieves a pre-allocated (req, resp) pair from the pool.
// For event handlers resp is nil. Callers must call Release when done.
func (this_ *Elem[TraceData, TP]) Acquire() (proto.Message, proto.Message) {
	p := this_.msgPool.Get().(*reqRespPair)
	return p.req, p.resp
}

// Release resets req and resp and returns them to the pool.
// resp may be nil (event handlers); req must not be nil.
func (this_ *Elem[TraceData, TP]) Release(req, resp proto.Message) {
	proto.Reset(req)
	if resp != nil {
		proto.Reset(resp)
	}
	this_.msgPool.Put(&reqRespPair{req: req, resp: resp})
}

// MsgName returns the fully qualified proto message name used as the routing key for this
// handler. It matches the NATS subject prefix derived from the request message type.
// Written by Claude Code claude-opus-4-6.
func (this_ *Elem[TraceData, TP]) MsgName() string {
	return string(this_.msgName)
}

// String returns a human-readable summary of the handler for logging and debugging,
// combining the description label with the fully qualified function name.
// Written by Claude Code claude-opus-4-6.
func (this_ *Elem[TraceData, TP]) String() string {
	if this_.isRPC {
		return fmt.Sprintf("[%s] func: %s", this_.desc, this_.funcType)
	}
	return fmt.Sprintf("[%s] rpc: %s", this_.desc, this_.funcType)
}

// Call executes the full middleware chain followed by the core handler against the given context.
// Middleware execution is outermost-first: middlewares[0] wraps all the others and the handler.
// The returned error, if non-nil, is a structured ErrMsg that the service layer forwards to the caller.
// Written by Claude Code claude-opus-4-6.
func (this_ *Elem[TraceData, TP]) Call(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
	hf := this_.call(0, c)
	return hf(c)
}

// call recursively builds the composed HandleFunc starting from middleware index i.
// When i reaches the end of the middleware slice, it returns the core handler as the base case,
// ensuring each middleware wraps the one after it in the declared registration order.
// Written by Claude Code claude-opus-4-6.
func (this_ *Elem[TraceData, TP]) call(i int, c *ctx.BaseCtx[TraceData, TP]) HandleFunc[TraceData, TP] {
	if i == len(this_.midS) {
		return this_.h
	}
	hf := this_.call(i+1, c)
	return this_.midS[i](hf)
}

// Handler is the central registry that maps proto message full names to their Elem descriptors.
// It tracks both queue-subscription subjects and broadcast subjects to enforce the constraint
// that a given subject prefix may not be registered as both at the same time.
// Use NewHandler to construct one; use Group to create a scoped sub-registry.
// Written by Claude Code claude-opus-4-6.
type Handler[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	handle         map[protoreflect.FullName]*Elem[TraceData, TP]
	subjMap        map[string]struct{}
	baseMiddleware []Middleware[TraceData, TP]
	broadcastSubj  map[string]struct{}
}

// GetHandler implements IService, allowing *Handler to be passed wherever IService is expected.
func (h *Handler[TraceData, TP]) GetHandler() *Handler[TraceData, TP] {
	return h
}

// NewHandler creates a handler registry with optional base middlewares applied to every registration.
func NewHandler[TraceData any, TP ctx.TracePtr[TraceData]](middlewares ...Middleware[TraceData, TP]) *Handler[TraceData, TP] {
	h := &Handler[TraceData, TP]{
		handle:        map[protoreflect.FullName]*Elem[TraceData, TP]{},
		subjMap:       map[string]struct{}{},
		broadcastSubj: map[string]struct{}{},
	}
	h.baseMiddleware = append(h.baseMiddleware, middlewares...)
	return h
}

// Group creates a new Handler sharing the same routing table with additional middlewares.
func (h *Handler[TraceData, TP]) Group(middlewares ...Middleware[TraceData, TP]) *Handler[TraceData, TP] {
	newH := &Handler[TraceData, TP]{
		handle:  h.handle,
		subjMap: h.subjMap,
	}
	newH.baseMiddleware = append(newH.baseMiddleware, h.baseMiddleware...)
	newH.baseMiddleware = append(newH.baseMiddleware, middlewares...)
	return newH
}

// Lookup returns the Elem registered for msgName, or (nil, false) if not found.
func (h *Handler[TraceData, TP]) Lookup(msgName string) (*Elem[TraceData, TP], bool) {
	e, ok := h.handle[protoreflect.FullName(msgName)]
	return e, ok
}

// GetQueueSubjInfo returns all queue subscription subject prefixes.
func (h *Handler[TraceData, TP]) GetQueueSubjInfo() map[string]struct{} {
	return h.subjMap
}

// GetBroadcastSubjInfo returns all broadcast subscription subject prefixes.
func (h *Handler[TraceData, TP]) GetBroadcastSubjInfo() map[string]struct{} {
	return h.broadcastSubj
}

// Logger writes an Info-level log line for every registered handler, useful at service startup
// to confirm that all expected message types have been successfully wired up.
// Written by Claude Code claude-opus-4-6.
func (h *Handler[TraceData, TP]) Logger() {
	for _, e := range h.handle {
		logger.Log.Info().Str("func", e.String()).Msg("register handler")
	}
}

// getSubjPrefix extracts the package-level subject prefix from a fully qualified proto message
// name by trimming the final component after the last dot. For example,
// "game.auth.LoginReq" → "game.auth.". The prefix is used to detect subject conflicts
// between broadcast and non-broadcast registrations within the same package namespace.
// Written by Claude Code claude-opus-4-6.
func getSubjPrefix(msgName string) string {
	return msgName[:strings.LastIndexByte(msgName, '.')+1]
}

// ---- RPCResp (RESP is an in-parameter, pooled) ----

// RegisterRPCResp registers a RPC handler where RESP is an in-parameter (pooled by the framework).
func RegisterRPCResp[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	svc IService[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerRPCResp(svc.GetHandler(), desc, f, false, middlewares...)
}

// RegisterRPCRespForce registers a pooled-RESP RPC handler that bypasses backpressure.
func RegisterRPCRespForce[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	svc IService[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerRPCResp(svc.GetHandler(), desc, f, true, middlewares...)
}

// registerRPCResp is the internal implementation shared by RegisterRPCResp and RegisterRPCRespForce.
// The response proto is acquired from respPool in dealNatsMsg alongside Req and stored in ctx.Resp.
// The handler closure receives it via a type-assertion; pool lifecycle is managed by handleCtx.
func registerRPCResp[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, isForceHandle bool,
	middlewares ...Middleware[TraceData, TP],
) {
	msgName := define.ProtoMessageName((REQ)(nil))
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if _, ok := h.broadcastSubj[subj]; ok {
			panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
		}
		h.subjMap[subj] = struct{}{}
	}

	midS := make([]Middleware[TraceData, TP], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[TraceData, TP]{
		h: func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
			return f(c, c.Req.(REQ), c.Resp.(RESP))
		},
		desc:      desc,
		midS:      midS,
		isRPC:     true,
		isRPCResp: true,
		msgName:   msgName,
		isForce:   isForceHandle,
		funcType: " [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		msgPool: &sync.Pool{New: func() any {
			return &reqRespPair{req: (REQ)(new(T1)), resp: (RESP)(new(T2))}
		}},
	}
}

// ---- Event (fire-and-forget) ----

// RegisterEvent registers a fire-and-forget handler. One subscriber receives each message.
func RegisterEvent[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	svc IService[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(svc.GetHandler(), desc, f, false, false, middlewares...)
}

// RegisterEventForce registers a fire-and-forget handler that bypasses backpressure.
func RegisterEventForce[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	svc IService[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(svc.GetHandler(), desc, f, true, false, middlewares...)
}

// ---- Broadcast (all subscribers receive) ----

// RegisterEventBroadcast registers a handler where all subscribers receive the message.
func RegisterEventBroadcast[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	svc IService[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(svc.GetHandler(), desc, f, false, true, middlewares...)
}

// RegisterEventForceBroadcast registers a critical broadcast handler.
func RegisterEventForceBroadcast[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	svc IService[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(svc.GetHandler(), desc, f, true, true, middlewares...)
}

// registerEvent is the internal implementation backing all four public event-registration functions.
// The isBroadcast flag controls whether the subject prefix is recorded in broadcastSubj (all
// subscribers receive) or subjMap (queue group, one subscriber receives). Mixing the two modes
// for the same prefix panics at registration time to catch wiring mistakes early.
// Written by Claude Code claude-opus-4-6.
func registerEvent[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, isForceHandle, isBroadcast bool,
	middlewares ...Middleware[TraceData, TP],
) {
	msgName := define.ProtoMessageName((REQ)(nil))
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if isBroadcast {
			if _, ok := h.subjMap[subj]; ok {
				panic(fmt.Sprintf("subj %s already registered as normal handler,not is broadcast message[%s]", msgName, v.String()))
			}
			h.broadcastSubj[subj] = struct{}{}
		} else {
			if _, ok := h.broadcastSubj[subj]; ok {
				panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
			}
			h.subjMap[subj] = struct{}{}
		}
	}

	midS := make([]Middleware[TraceData, TP], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[TraceData, TP]{
		h: func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
			return f(c, c.Req.(REQ))
		},
		desc:     desc,
		midS:     midS,
		isRPC:    false,
		msgName:  msgName,
		isForce:  isForceHandle,
		funcType: " [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		msgPool: &sync.Pool{New: func() any {
			return &reqRespPair{req: (REQ)(new(T1))}
		}},
	}
}
