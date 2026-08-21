package service

import (
	"reflect"
	"sync"
	"time"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/task_group"
	"github.com/ravinggo/game/common/timer"
)

// timeTask wraps a TaskGroup with housekeeping state for idle cleanup.
// Written by Claude Code claude-opus-4-6.
type timeTask[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	task_group.TaskGroup[CE[TraceData, TP]]
	lastDealTime int64
	roleID       int64
	uniqueID     int64
}

// OneHashOneGoService creates a dedicated goroutine (via TaskGroup) for each unique hash,
// guaranteeing in-order processing per hash. Goroutines are created on demand and
// cleaned up after 30 s of inactivity. Messages with hash == 0 spawn a plain goroutine.
// Use this when per-entity ordering matters and goroutine count can grow unbounded.
// Written by Claude Code claude-opus-4-6.
type OneHashOneGoService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	*BaseService[TraceData, TP]
	el            *eventloop.DoubleBuffQueue
	taskMap       map[int64]*timeTask[TraceData, TP]
	taskGroupPool *sync.Pool
}

// NewOneHashOneGoService creates a service using the one-hash-one-goroutine dispatch mode.
// Written by Claude Code claude-opus-4-6.
func NewOneHashOneGoService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	c Config[TraceData, TP],
) *OneHashOneGoService[TraceData, TP] {
	base := NewBaseService[TraceData, TP](natsUrls, c)
	base.h = handler.NewHandler[TraceData](c.allMiddlewares()...)
	s := &OneHashOneGoService[TraceData, TP]{
		BaseService:   base,
		el:            eventloop.NewDoubleBuffQueue(c.LockQueueThread),
		taskMap:       map[int64]*timeTask[TraceData, TP]{},
		taskGroupPool: objectpool.GetTypePool[timeTask[TraceData, TP]](),
	}
	s.dispatch = s.doDispatch
	return s
}

// PostTaskCloneCtx posts f to the EventLoop, cloning c's TraceData into a new pooled ctx.
// The caller retains ownership of c. f runs sequentially with all messages sharing the same RoleID.
func (s *OneHashOneGoService[TraceData, TP]) PostTaskCloneCtx(c *ctx.BaseCtx[TraceData, TP], f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil || c == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.TD = c.TD
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	s.el.PostEventQueue(CE[TraceData, TP]{Ctx: newCtx, Func: f})
}

// PostTaskWithRoleId posts f to the TaskGroup for roleId using a fresh pooled ctx.
// f runs sequentially with all other messages sharing the same roleId.
func (s *OneHashOneGoService[TraceData, TP]) PostTaskWithRoleId(roleId int64, f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.GetTrace().SetRoleID(roleId)
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	s.el.PostEventQueue(CE[TraceData, TP]{Ctx: newCtx, Func: f})
}

// Start subscribes to NATS and begins the EventLoop.
// Written by Claude Code claude-opus-4-6.
func (s *OneHashOneGoService[TraceData, TP]) Start(f func(any)) {
	if f == nil {
		f = func(e any) {
			logger.Log.Warn().Str("type", reflect.TypeOf(e).String()).Any("data", e).Msg("unknown event")
		}
	}
	s.h.Logger()
	s.Subscribe()
	s.el.Start(
		func(e any) {
			switch c := e.(type) {
			case CE[TraceData, TP]:
				s.dealCE(c)
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
func (s *OneHashOneGoService[TraceData, TP]) Stop() {
	s.natsCluster.Close()
	s.el.Stop()
	s.natsCluster.Shutdown()
}

// taskFunc is the worker callback for each per-RoleID TaskGroup.
// When CE.Func is set: acquire ctx (from pool if nil), call Func, return ctx to pool.
// When CE.Func is nil: if Elem is present, invoke handleCtx.
// Written by Claude Code claude-opus-4-6.
func (s *OneHashOneGoService[TraceData, TP]) taskFunc(e task_group.TaskGroupElem[CE[TraceData, TP]]) {
	defer safego.Recover()
	if e.Data.Func != nil {
		data := e.Data.Ctx
		if data == nil {
			data = s.GetCtxFromPool()
		}
		if err := s.applyServiceMiddles(e.Data.Func)(data); err != nil {
			data.Warn().Err(err).Msg("PostTask func error")
		}
		s.PutCtxToPool(data)
		return
	}
	if e.Data.Elem != nil {
		s.handleCtx(e.Data.Ctx, e.Data.Elem)
	}
}

// doDispatch routes an incoming message: non-zero RoleID events are posted to the EventLoop
// for per-RoleID serialisation; zero-RoleID messages run in their own plain goroutine.
// Written by Claude Code claude-opus-4-6.
func (s *OneHashOneGoService[TraceData, TP]) doDispatch(
	c *ctx.BaseCtx[TraceData, TP], elem *handler.Elem[TraceData, TP],
) {
	roleID := c.GetTrace().GetRoleID()
	if roleID != 0 {
		s.el.PostEventQueue(CE[TraceData, TP]{Ctx: c, Elem: elem})
		return
	}
	safego.Go(
		func() {
			defer safego.RecoverWithLogger(c)
			s.handleCtx(c, elem)
		},
	)
}

// dealCE dispatches an EventLoop CE event: RoleID==0 runs inline (sequential),
// RoleID!=0 routes to the per-RoleID dynamic TaskGroup.
// Written by Claude Code claude-opus-4-6.
func (s *OneHashOneGoService[TraceData, TP]) dealCE(c CE[TraceData, TP]) {
	roleID := c.Ctx.GetTrace().GetRoleID()
	if roleID == 0 {
		if c.Func != nil {
			if err := s.applyServiceMiddles(c.Func)(c.Ctx); err != nil {
				c.Ctx.Warn().Err(err).Msg("PostTask func error")
			}
			s.PutCtxToPool(c.Ctx)
		} else {
			s.handleCtx(c.Ctx, c.Elem)
		}
		return
	}
	tg, ok := s.taskMap[roleID]
	if !ok {
		tg = s.taskGroupPool.Get().(*timeTask[TraceData, TP])
		tg.SetTaskFunc(s.taskFunc)
		tg.SetMaxCap(128)
		tg.roleID = roleID
		tg.uniqueID = time.Now().UnixNano()
		s.taskMap[roleID] = tg
		s.scheduleIdleCleanup(tg)
	}
	tg.lastDealTime = timer.GetLowPrecisionTime()
	tg.PutForce(c, nil)
}

// scheduleIdleCleanup registers a periodic timer on the EventLoop. When it fires,
// the current TaskGroup for roleID is looked up from taskMap; if it has not processed
// a message within the idle timeout it is evicted and returned to the pool for reuse.
// The timeout is set via IdleCleanupTimeoutOption and defaults to 30 seconds.
func (s *OneHashOneGoService[TraceData, TP]) scheduleIdleCleanup(tg *timeTask[TraceData, TP]) {
	timeout := s.cnf.IdleCleanupTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	thresholdSecs := int64(timeout.Seconds())
	s.el.Ticker(
		timeout, func() bool {
			newTg, ok := s.taskMap[tg.roleID]
			if !ok || tg.uniqueID != newTg.uniqueID {
				return false
			}
			if timer.GetLowPrecisionTime()-tg.lastDealTime > thresholdSecs {
				delete(s.taskMap, tg.roleID)
				tg.roleID = 0
				tg.lastDealTime = 0
				s.taskGroupPool.Put(tg)
				return false
			}
			return true
		},
	)
}
