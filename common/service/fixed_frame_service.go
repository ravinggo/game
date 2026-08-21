package service

import (
	"reflect"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/logger"
)

// frameLooper abstracts the two frame-loop pacing strategies
// (eventloop.AbsFrameLoop and eventloop.TickerFrameLoop) so the service logic
// shared by FixedFrameAbsService and FixedFrameTickerService is written once.
type frameLooper interface {
	PostEventQueue(e any)
	Start(frame eventloop.FrameFunc, handle func(event any), endF ...func())
	Stop()
	Stopped() bool
}

// fixedFrameService is the shared implementation of the fixed-frame services.
// All NATS messages and posted tasks are buffered and processed sequentially
// on the single frame-loop goroutine at each frame boundary, before the
// per-frame callback runs.
type fixedFrameService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	*BaseService[TraceData, TP]
	fl frameLooper
}

// PostTaskCloneCtx posts f for execution at the next frame boundary, cloning
// c's TraceData into a new pooled ctx. The caller retains ownership of c.
func (s *fixedFrameService[TraceData, TP]) PostTaskCloneCtx(c *ctx.BaseCtx[TraceData, TP], f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil || c == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.TD = c.TD
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	s.fl.PostEventQueue(CE[TraceData, TP]{Ctx: newCtx, Func: f})
}

// PostTaskWithRoleId posts f for execution at the next frame boundary using a
// fresh pooled ctx with roleId set.
func (s *fixedFrameService[TraceData, TP]) PostTaskWithRoleId(roleId int64, f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.GetTrace().SetRoleID(roleId)
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	s.fl.PostEventQueue(CE[TraceData, TP]{Ctx: newCtx, Func: f})
}

// PostFunc posts a plain func() for execution at the next frame boundary on
// the frame-loop goroutine.
func (s *fixedFrameService[TraceData, TP]) PostFunc(f func()) {
	if f == nil {
		return
	}
	s.fl.PostEventQueue(f)
}

// handleEvent builds the event handler passed to the frame loop: CE events run
// the middleware chain / registered handler, anything else goes to f.
func (s *fixedFrameService[TraceData, TP]) handleEvent(f func(any)) func(any) {
	return func(e any) {
		switch c := e.(type) {
		case CE[TraceData, TP]:
			if c.Func != nil {
				if err := s.ApplyServiceMiddles(c.Func)(c.Ctx); err != nil {
					c.Ctx.Warn().Err(err).Msg("PostTask func error")
				}
				s.PutCtxToPool(c.Ctx)
			} else {
				s.HandleCtx(c.Ctx, c.Elem)
			}
		default:
			f(e)
		}
	}
}

// Start subscribes to NATS and begins the frame loop. frame is invoked once
// per frame after all buffered messages of that frame have been processed;
// it may be nil when only frame-aligned message processing is needed.
// f receives events of unknown type; a warning logger is used when nil.
func (s *fixedFrameService[TraceData, TP]) Start(frame eventloop.FrameFunc, f func(any)) {
	if f == nil {
		f = func(e any) {
			logger.Log.Warn().Str("type", reflect.TypeOf(e).String()).Any("data", e).Msg("unknown event")
		}
	}
	s.h.Logger()
	s.Subscribe()
	s.fl.Start(frame, s.handleEvent(f))
}

// Stop drains the frame loop before shutting down NATS connections.
func (s *fixedFrameService[TraceData, TP]) Stop() {
	s.natsCluster.Close()
	s.fl.Stop()
	s.natsCluster.Shutdown()
}
