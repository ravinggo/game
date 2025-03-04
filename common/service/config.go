package service

import (
	"time"

	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
)

// HashRunMode 0: FixedHashPoolMode, 1: OneHashOneGo
// FixedHashPoolMode use hash to run task,
// OneHashOneGo use one hash one goroutine
type HashRunMode int

const (
	// OneHashOneGo one hash one goroutine
	OneHashOneGo HashRunMode = 0

	// FixedHashPoolMode fixed((runtime.NumCPU()+1)*1024) task group pool
	// by ToHash() distribute task to task_group.TaskGroup
	FixedHashPoolMode HashRunMode = 1
)

type TaskRunMode int

const (
	// TaskPool all task run in task_group.TaskPool
	TaskPool TaskRunMode = 0
	// OneTaskOneGo one task one goroutine
	OneTaskOneGo TaskRunMode = 1
)

type config[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	lockQueueThread bool
	hashRunMode     HashRunMode
	taskRunMode     TaskRunMode
	rpcTimeout      time.Duration
	middles         []handler.Middleware[TraceData, TP]
}

type Option[TraceData any, TP ctx.TracePtr[TraceData]] func(*config[TraceData, TP])

func LockQueueThreadOption[TraceData any, TP ctx.TracePtr[TraceData]]() Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.lockQueueThread = true
	}
}

func HashRunModeOption[TraceData any, TP ctx.TracePtr[TraceData]](hm HashRunMode) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.hashRunMode = hm
	}
}

func TaskRunModeOption[TraceData any, TP ctx.TracePtr[TraceData]](tm TaskRunMode) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.taskRunMode = tm
	}
}

func RPCTimeoutOption[TraceData any, TP ctx.TracePtr[TraceData]](timeout time.Duration) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.rpcTimeout = timeout
	}
}

func MiddlesOption[TraceData any, TP ctx.TracePtr[TraceData]](middles ...handler.Middleware[TraceData, TP]) Option[TraceData, TP] {
	return func(c *config[TraceData, TP]) {
		c.middles = append(c.middles, middles...)
	}
}
