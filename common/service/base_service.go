package service

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ravinggo/objectpool"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/timer"
)

var ErrNotFoundHandler = berror.NewProtocolStr("not found handler")

// CE is posted to an EventLoop to carry a parsed request or a PostTask callback.
// When Func is non-nil the taskFunc calls it instead of handleCtx, then returns Ctx to pool.
// RoleID is read from Ctx.GetTrace().GetRoleID() at the point of routing.
// Written by Claude Code claude-opus-4-6.
type CE[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	Ctx  *ctx.BaseCtx[TraceData, TP]
	Elem *handler.Elem[TraceData, TP]
	Func func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg
}

// BaseService holds shared infrastructure. It does not own an EventLoop or a handler registry;
// concrete service types provide both only when their dispatch strategy requires them.
// Written by Claude Code claude-opus-4-6.
type BaseService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	natsCluster    *natsclient.ClusterClient
	ctxPool        *sync.Pool
	serverId       int64
	serverType     string
	cnf            Config[TraceData, TP]
	serviceMiddles []handler.Middleware[TraceData, TP]
	h              *handler.Handler[TraceData, TP]
	// dispatch is set by each concrete service constructor.
	dispatch func(*ctx.BaseCtx[TraceData, TP], *handler.Elem[TraceData, TP])
}

// GetHandler returns the shared handler registry used by this service.
func (s *BaseService[TraceData, TP]) GetHandler() *handler.Handler[TraceData, TP] {
	return s.h
}

// RegisterRPCResp registers a request/response handler on this service's registry.
// The handler owns registration state; the service method keeps service wiring local
// to the service that will receive the message.
func (s *BaseService[TraceData, TP]) RegisterRPCResp[REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T1 any, T2 any](
	desc string,
	f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg,
	middlewares ...handler.Middleware[TraceData, TP],
) {
	s.h.RegisterRPCResp(desc, f, false, middlewares...)
}

// RegisterRPCRespForce registers a force request/response handler on this service.
func (s *BaseService[TraceData, TP]) RegisterRPCRespForce[REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T1 any, T2 any](
	desc string,
	f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg,
	middlewares ...handler.Middleware[TraceData, TP],
) {
	s.h.RegisterRPCResp(desc, f, true, middlewares...)
}

// RegisterEvent registers a queue-group event handler on this service.
func (s *BaseService[TraceData, TP]) RegisterEvent[REQ define.ProtoMessagePtr[T1], T1 any](
	desc string,
	f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg,
	middlewares ...handler.Middleware[TraceData, TP],
) {
	s.h.RegisterEvent(desc, f, middlewares...)
}

// RegisterEventForce registers a force queue-group event handler on this service.
func (s *BaseService[TraceData, TP]) RegisterEventForce[REQ define.ProtoMessagePtr[T1], T1 any](
	desc string,
	f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg,
	middlewares ...handler.Middleware[TraceData, TP],
) {
	s.h.RegisterEventForce(desc, f, middlewares...)
}

// RegisterEventBroadcast registers an event delivered to every subscriber.
func (s *BaseService[TraceData, TP]) RegisterEventBroadcast[REQ define.ProtoMessagePtr[T1], T1 any](
	desc string,
	f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg,
	middlewares ...handler.Middleware[TraceData, TP],
) {
	s.h.RegisterEventBroadcast(desc, f, middlewares...)
}

// RegisterEventForceBroadcast registers a force event delivered to every subscriber.
func (s *BaseService[TraceData, TP]) RegisterEventForceBroadcast[REQ define.ProtoMessagePtr[T1], T1 any](
	desc string,
	f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg,
	middlewares ...handler.Middleware[TraceData, TP],
) {
	s.h.RegisterEventForceBroadcast(desc, f, middlewares...)
}

// NewBaseService initialises shared BaseService infrastructure: NATS cluster client,
// context pool, server identity, and a low-precision timer. It is called by every
// concrete service constructor before any dispatch strategy is configured.
// Written by Claude Code claude-opus-4-6.
func NewBaseService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	dispatchFunc func(*ctx.BaseCtx[TraceData, TP], *handler.Elem[TraceData, TP]),
	c Config[TraceData, TP],
) *BaseService[TraceData, TP] {
	if c.RpcTimeout <= 0 {
		c.RpcTimeout = time.Second * 10
	}
	s := &BaseService[TraceData, TP]{
		natsCluster: natsclient.NewClusterClient(natsUrls, c.RpcTimeout),
		serverId:    baseenv.GetConfig().ServerId,
		serverType:  baseenv.GetConfig().ServerType,
		ctxPool:     objectpool.GetTypePool[ctx.BaseCtx[TraceData, TP]](),
		cnf:         c,
		h:           handler.NewHandler[TraceData](c.allMiddlewares()...),
		dispatch:    dispatchFunc,
	}

	s.serviceMiddles = c.serviceMiddlewares()
	timer.StartLowPrecisionTime()
	return s
}

func (s *BaseService[TraceData, TP]) GetConfig() Config[TraceData, TP] {
	return s.cnf
}

// applyServiceMiddles wraps f with all service-scoped middlewares in declaration order.
// middlewares[0] is the outermost wrapper. Returns f unchanged when no service middlewares are set.
func (s *BaseService[TraceData, TP]) applyServiceMiddles(
	f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg,
) func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
	for i := len(s.serviceMiddles) - 1; i >= 0; i-- {
		f = s.serviceMiddles[i](f)
	}
	return f
}

// GetCtxFromPool acquires a BaseCtx from the sync.Pool for reuse on the hot path.
// If InitCtxOption was provided, it is called to initialise the context before returning it.
// Written by Claude Code claude-opus-4-6.
func (s *BaseService[TraceData, TP]) GetCtxFromPool() *ctx.BaseCtx[TraceData, TP] {
	c := s.ctxPool.Get().(*ctx.BaseCtx[TraceData, TP])
	if s.cnf.InitCtx != nil {
		s.cnf.InitCtx(c)
	}
	return c
}

// PutCtxToPool resets the given BaseCtx and returns it to the sync.Pool.
// The context must not be used after this call.
// Written by Claude Code claude-opus-4-6.
func (s *BaseService[TraceData, TP]) PutCtxToPool(c *ctx.BaseCtx[TraceData, TP]) {
	c.Reset()
	s.ctxPool.Put(c)
}

// GetNatsCluster returns the underlying NATS cluster client for direct publish/request calls.
// Written by Claude Code claude-opus-4-6.
func (s *BaseService[TraceData, TP]) GetNatsCluster() *natsclient.ClusterClient {
	return s.natsCluster
}

// Stop closes NATS connections. Services that own an EventLoop must override this
// to call el.Stop() between natsCluster.Close() and natsCluster.Shutdown().
// Written by Claude Code claude-opus-4-6.
func (s *BaseService[TraceData, TP]) Stop() {
	s.natsCluster.Close()
	s.natsCluster.Shutdown()
}

// handleCtx runs the handler chain for a single request context, then recycles Req and Resp
// back to their pools together, and returns the context to the pool when done.
func (s *BaseService[TraceData, TP]) handleCtx(c *ctx.BaseCtx[TraceData, TP], e *handler.Elem[TraceData, TP]) {
	s.call(c, e)
	if c.Req != nil {
		e.Release(c.Req, c.Resp)
	}
	s.PutCtxToPool(c)
}

// call invokes the handler middleware chain and sends the NATS reply when one is expected.
// Resp pool lifecycle is managed by handleCtx; call() only performs the network reply.
func (s *BaseService[TraceData, TP]) call(c *ctx.BaseCtx[TraceData, TP], e *handler.Elem[TraceData, TP]) {
	err := e.Call(c)
	if err != nil {
		if e.IsRPC() {
			err = natsclient.NatsMsgReplyError(c.NatsMsg, err)
			if err != nil {
				logger.Log.Warn().Err(err).Msg("NatsMsgReplyError fail")
			}
		}
		return
	}
	if c.NatsMsg != nil && c.Resp != nil {
		err = natsclient.NatsMsgReply(c.NatsMsg, s.cnf.RespFirst, c.Resp, c.OtherResp...)
		if err != nil {
			logger.Log.Warn().Err(err).Msg("NatsMsgReply fail")
		}
	}
}

// Subscribe sets up NATS queue and broadcast subscriptions for all registered handlers.
// Queue subjects use serverId-scoped wildcards; broadcast subjects are subscribed globally
// and, when serverId != 0, also with a serverId-specific wildcard.
// Written by Claude Code claude-opus-4-6.
func (s *BaseService[TraceData, TP]) Subscribe() {
	h := s.h
	subjInfo := h.GetQueueSubjInfo()
	serverId := baseenv.GetConfig().ServerId
	for subj := range subjInfo {
		if serverId == 0 {
			subj = subj + ">"
		} else {
			subj = subj + strconv.FormatInt(serverId, 10) + ".>"
		}
		s.natsCluster.QueueSubscribeAll(subj, s.DealNatsMsg)
		logger.Log.Info().Str("subj", subj).Msg("subscribe queue topic")
	}
	broadcastSubjInfo := h.GetBroadcastSubjInfo()
	for subj := range broadcastSubjInfo {
		subjTop := subj + ">"
		s.natsCluster.SubscribeAll(subjTop, s.DealNatsMsg)
		logger.Log.Info().Str("subjTop", subjTop).Msg("subscribe broadcast top topic")
		if serverId != 0 {
			subjServerId := subj + strconv.FormatInt(serverId, 10) + ".>"
			s.natsCluster.SubscribeAll(subjServerId, s.DealNatsMsg)
			logger.Log.Info().Str("subjServerId", subjTop).Msg("subscribe broadcast topic")
		}
	}
}

// DealNatsMsg is the shared NATS entry point. It parses the wire format and
// calls s.dispatch — the concrete service owns all routing decisions.
// Written by Claude Code claude-opus-4-6.
func (s *BaseService[TraceData, TP]) DealNatsMsg(msg *nats.Msg) {
	if s.cnf.DispatchHook != nil && s.cnf.DispatchHook(msg) {
		return
	}
	msgName := msg.Subject
	index := strings.LastIndexByte(msgName, '.')
	if index == -1 {
		return
	}
	if baseenv.GetConfig().ServerId != 0 {
		index1 := strings.LastIndexByte(msg.Subject[:index], '.')
		b := objectpool.GetBytes(len(msgName))
		defer objectpool.PutBytes(b)
		b.WriteString(msg.Subject[:index1])
		b.WriteString(msg.Subject[index:])
		msgName = b.String()
	}

	elem, ok := s.h.Lookup(msgName)
	if !ok {
		logger.Log.Warn().Str("msgName", msgName).Str("subj", msg.Subject).Msg("msg not registered")
		if msg.Reply != "" {
			_ = natsclient.NatsMsgReplyError(msg, ErrNotFoundHandler)
		}
		return
	}

	data := msg.Data
	if len(data) < 2 {
		return
	}

	traceSize := int(data[0]) | int(data[1])<<8
	c := s.GetCtxFromPool()
	traceCtx := c.GetTrace()
	if traceSize > 0 && traceCtx != nil {
		err := traceCtx.TraceMarshalFrom(msg.Data[2 : 2+traceSize])
		if err != nil {
			if msg.Reply == "" {
				e := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(err))
				if e != nil {
					logger.Log.Error().Err(e).Msg("nats reply error")
				}
			}
			return
		}
	}

	c.Req, c.Resp = elem.Acquire()
	if elem.IsRPC() {
		c.NatsMsg = msg
	}
	err := define.ProtoUnmarshal(data[2+traceSize:], c.Req)
	if err != nil {
		if msg.Reply == "" {
			e := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(err))
			if e != nil {
				c.Error().Err(e).Msg("nats reply error")
			}
		}
		return
	}

	s.dispatch(c, elem)
}

// ReplyTaskPoolFull sends a "task pool full" error reply when a handler cannot be queued.
// Written by Claude Code claude-opus-4-6.
func ReplyTaskPoolFull[TraceData any, TP ctx.TracePtr[TraceData]](c *ctx.BaseCtx[TraceData, TP]) {
	if c.NatsMsg != nil && c.NatsMsg.Reply != "" {
		err := natsclient.NatsMsgReplyError(c.NatsMsg, berror.NewProtocolStr("task pool full"))
		if err != nil {
			logger.Log.Error().Err(err).Msg("nats reply error")
		}
	}
}
