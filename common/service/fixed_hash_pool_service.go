package service

import (
	"math/rand/v2"
	"runtime"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/task_group"
)

// FixedHashPoolService routes messages to a pre-allocated pool of TaskGroups.
//   - hash != 0 → taskGroupHash[hash % poolSize] (hash-ordered per bucket)
//   - hash == 0 → randomly assigned TaskGroup (even distribution)
//
// Pool size is fixed at (NumCPU rounded up to even) × 1024.
// Use this for bounded-goroutine, hash-ordered processing.
// Written by Claude Code claude-opus-4-6.
type FixedHashPoolService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	*BaseService[TraceData, TP]
	taskGroupHash []task_group.TaskGroup[CE[TraceData, TP]]
	taskPoolMark  uint64
}

// NewFixedHashPoolService creates a service with a fixed hash-partitioned TaskGroup pool.
// Written by Claude Code claude-opus-4-6.
func NewFixedHashPoolService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	c Config[TraceData, TP],
) *FixedHashPoolService[TraceData, TP] {
	base := NewBaseService[TraceData, TP](natsUrls, c)
	base.h = handler.NewHandler[TraceData](c.allMiddlewares()...)
	s := &FixedHashPoolService[TraceData, TP]{
		BaseService: base,
	}

	numCpu := uint64(runtime.NumCPU())
	if numCpu&1 == 1 {
		numCpu++
	}
	poolSize := numCpu * 1024
	s.taskPoolMark = poolSize - 1
	s.taskGroupHash = make([]task_group.TaskGroup[CE[TraceData, TP]], poolSize)
	for i := range s.taskGroupHash {
		s.taskGroupHash[i].SetMaxCap(128)
		s.taskGroupHash[i].SetTaskFunc(s.taskFunc)
	}

	s.dispatch = s.doDispatch
	return s
}

// taskFunc is the worker callback passed to each TaskGroup.
// When CE.Func is set: acquire ctx (from pool if nil), call Func, return ctx to pool.
// When CE.Func is nil: if Elem is present, invoke handleCtx.
// Written by Claude Code claude-opus-4-6.
func (s *FixedHashPoolService[TraceData, TP]) taskFunc(e task_group.TaskGroupElem[CE[TraceData, TP]]) {
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

// doDispatch selects a TaskGroup bucket for the incoming message.
// Routing is by int64 RoleID; if RoleID is zero a random bucket is chosen.
// Force handlers bypass the capacity limit.
// Written by Claude Code claude-opus-4-6.
func (s *FixedHashPoolService[TraceData, TP]) doDispatch(
	c *ctx.BaseCtx[TraceData, TP], elem *handler.Elem[TraceData, TP],
) {
	roleID := c.GetTrace().GetRoleID()
	var bucket uint64
	if roleID == 0 {
		bucket = rand.Uint64() & s.taskPoolMark
	} else {
		if roleID < 0 {
			roleID = -roleID
		}
		bucket = uint64(roleID) & s.taskPoolMark
	}
	if elem.IsForce() {
		s.taskGroupHash[bucket].PutForce(CE[TraceData, TP]{Ctx: c, Elem: elem}, nil)
	} else {
		if !s.taskGroupHash[bucket].Put(CE[TraceData, TP]{Ctx: c, Elem: elem}, nil) {
			ReplyTaskPoolFull(c)
			c.Warn().Msg("task group full")
			s.PutCtxToPool(c)
		}
	}
}

// PostTaskCloneCtx posts f to the bucket derived from c's RoleID, cloning c's TraceData into a new pooled ctx.
// The caller retains ownership of c. f runs sequentially with all messages in the same bucket.
func (s *FixedHashPoolService[TraceData, TP]) PostTaskCloneCtx(c *ctx.BaseCtx[TraceData, TP], f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil || c == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.TD = c.TD
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	roleID := newCtx.GetTrace().GetRoleID()
	var bucket uint64
	if roleID == 0 {
		bucket = rand.Uint64() & s.taskPoolMark
	} else {
		r := roleID
		if r < 0 {
			r = -r
		}
		bucket = uint64(r) & s.taskPoolMark
	}
	s.taskGroupHash[bucket].PutForce(CE[TraceData, TP]{Ctx: newCtx, Func: f}, nil)
}

// PostTaskWithRoleId posts f to the bucket derived from roleId using a fresh pooled ctx.
// f runs sequentially with all other messages in the same bucket.
func (s *FixedHashPoolService[TraceData, TP]) PostTaskWithRoleId(roleId int64, f func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg) {
	if f == nil {
		return
	}
	newCtx := s.GetCtxFromPool()
	newCtx.GetTrace().SetRoleID(roleId)
	newCtx.GetTrace().SetServerIdAndType(s.serverId, s.serverType)
	var bucket uint64
	if roleId == 0 {
		bucket = rand.Uint64() & s.taskPoolMark
	} else {
		r := roleId
		if r < 0 {
			r = -r
		}
		bucket = uint64(r) & s.taskPoolMark
	}
	s.taskGroupHash[bucket].PutForce(CE[TraceData, TP]{Ctx: newCtx, Func: f}, nil)
}

// Start subscribes to NATS. No EventLoop is used; all work runs in TaskGroup goroutines.
// Written by Claude Code claude-opus-4-6.
func (s *FixedHashPoolService[TraceData, TP]) Start(_ func(any)) {
	s.h.Logger()
	s.Subscribe()
	logger.Log.Info().Msg("FixedHashPoolService started")
}
