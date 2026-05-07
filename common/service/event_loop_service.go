package service

import (
	"reflect"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
)

// EventLoopService runs all messages sequentially on a single EventLoop goroutine.
// Every message — regardless of hash — is processed in arrival order.
// Use this when strict global ordering of all requests is required.
// Written by Claude Code claude-opus-4-6.
type EventLoopService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	*BaseService[TraceData, TP]
	el *eventloop.DoubleBuffQueue
}

// NewEventLoopService creates a service where every message is processed sequentially on the EventLoop.
// Written by Claude Code claude-opus-4-6.
func NewEventLoopService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	ops ...Option[TraceData, TP],
) *EventLoopService[TraceData, TP] {
	c := buildConfig(ops)
	base := newBaseService[TraceData, TP](natsUrls, c)
	base.h = handler.NewHandler[TraceData](c.allMiddlewares()...)
	s := &EventLoopService[TraceData, TP]{
		BaseService: base,
		el:          eventloop.NewDoubleBuffQueue(c.lockQueueThread),
	}
	s.dispatch = func(c *ctx.BaseCtx[TraceData, TP], elem *handler.Elem[TraceData, TP]) {
		s.el.PostEventQueue(ce[TraceData, TP]{Ctx: c, Elem: elem})
	}
	return s
}

// PostTaskCloneCtx posts f for sequential execution on the EventLoop, cloning c's TraceData into a new pooled ctx.
// The caller retains ownership of c.
func (s *EventLoopService[TraceData, TP]) PostTaskCloneCtx(c *ctx.BaseCtx[TraceData, TP], f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil || c == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.TD = c.TD
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	s.el.PostEventQueue(ce[TraceData, TP]{Ctx: newCtx, Func: f})
}

// PostTaskWithRoleId posts f for sequential execution on the EventLoop using a fresh pooled ctx with roleId set.
func (s *EventLoopService[TraceData, TP]) PostTaskWithRoleId(roleId int64, f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.GetTrace().SetRoleID(roleId)
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	s.el.PostEventQueue(ce[TraceData, TP]{Ctx: newCtx, Func: f})
}

// Start subscribes to NATS and begins the EventLoop.
// Written by Claude Code claude-opus-4-6.
func (s *EventLoopService[TraceData, TP]) Start(f func(any)) {
	if f == nil {
		f = func(e any) {
			logger.Log.Warn().Str("type", reflect.TypeOf(e).String()).Any("data", e).Msg("unknown event")
		}
	}
	s.h.Logger()
	s.subscribe()
	s.el.Start(
		func(e any) {
			switch c := e.(type) {
			case ce[TraceData, TP]:
				if c.Func != nil {
					if err := s.applyServiceMiddles(c.Func)(c.Ctx); err != nil {
						c.Ctx.Warn().Err(err).Msg("PostTask func error")
					}
					s.PutCtxToPool(c.Ctx)
				} else {
					s.handleCtx(c.Ctx, c.Elem)
				}
			case func():
				c()
			default:
				f(e)
			}
		},
	)
}

// Stop drains the EventLoop before shutting down NATS connections.
// Written by Claude Code claude-opus-4-6.
func (s *EventLoopService[TraceData, TP]) Stop() {
	s.natsCluster.Close()
	s.el.Stop()
	s.natsCluster.Shutdown()
}
