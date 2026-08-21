package service

import (
	"runtime"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/task_group"
)

// TaskPoolService dispatches all messages to a fixed-size worker pool.
// There is no hash-based ordering; any worker may handle any message.
// Pool size is (NumCPU rounded up to even) × 1024 workers.
// Use this for maximum throughput when ordering is not required.
// Written by Claude Code claude-opus-4-6.
type TaskPoolService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	*BaseService[TraceData, TP]
	taskPool *task_group.TaskPool
}

// NewTaskPoolService creates a service backed by a fixed worker pool.
// Written by Claude Code claude-opus-4-6.
func NewTaskPoolService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	c Config[TraceData, TP],
) *TaskPoolService[TraceData, TP] {
	base := NewBaseService[TraceData, TP](natsUrls, c)
	base.h = handler.NewHandler[TraceData](c.allMiddlewares()...)
	s := &TaskPoolService[TraceData, TP]{
		BaseService: base,
	}

	numCpu := uint64(runtime.NumCPU())
	if numCpu&1 == 1 {
		numCpu++
	}
	poolSize := int64(numCpu * 1024)
	s.taskPool = task_group.NewTaskPool(poolSize, poolSize*10)

	s.dispatch = s.doDispatch
	return s
}

// PostTaskCloneCtx posts f to the worker pool, cloning c's TraceData into a new pooled ctx.
// The caller retains ownership of c.
func (s *TaskPoolService[TraceData, TP]) PostTaskCloneCtx(c *ctx.BaseCtx[TraceData, TP], f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil || c == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.TD = c.TD
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	wrapped := s.applyServiceMiddles(f)
	s.taskPool.PutForce(
		func() {
			if err := wrapped(newCtx); err != nil {
				newCtx.Warn().Err(err).Msg("PostTaskCloneCtx func error")
			}
			s.PutCtxToPool(newCtx)
		},
	)
}

// PostTaskWithRoleId posts f to the worker pool using a fresh pooled ctx with roleId set.
func (s *TaskPoolService[TraceData, TP]) PostTaskWithRoleId(roleId int64, f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.GetTrace().SetRoleID(roleId)
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	wrapped := s.applyServiceMiddles(f)
	s.taskPool.PutForce(
		func() {
			if err := wrapped(newCtx); err != nil {
				newCtx.Warn().Err(err).Msg("PostTaskWithRoleId func error")
			}
			s.PutCtxToPool(newCtx)
		},
	)
}

// Start subscribes to NATS. No EventLoop is used; all work runs in TaskPool goroutines.
// Written by Claude Code claude-opus-4-6.
func (s *TaskPoolService[TraceData, TP]) Start(_ func(any)) {
	s.h.Logger()
	s.Subscribe()
	logger.Log.Info().Msg("TaskPoolService started")
}

// doDispatch submits a message handler to the worker pool. Force handlers bypass the
// capacity limit; when the pool is full for normal handlers a pool-full reply is sent
// and the context is recycled.
// Written by Claude Code claude-opus-4-6.
func (s *TaskPoolService[TraceData, TP]) doDispatch(
	c *ctx.BaseCtx[TraceData, TP], elem *handler.Elem[TraceData, TP],
) {
	if elem.IsForce() {
		s.taskPool.PutForce(func() { s.handleCtx(c, elem) })
		return
	}
	if !s.taskPool.Put(func() { s.handleCtx(c, elem) }) {
		ReplyTaskPoolFull(c)
		c.Warn().Msg("task pool full")
		s.PutCtxToPool(c)
	}
}
