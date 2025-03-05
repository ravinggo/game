package service

import (
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/natsclient"
)

type HookUserMsg[T1 any, US natsclient.ServerUserSubjectPtr[T1]] = func(us US, msgName string, msgData []byte, msg *nats.Msg)

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

type ServerUserOption[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] func(*serverUserConfig[T1, TraceData, TP, US])

type serverUserConfig[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] struct {
	options     []Option[TraceData, TP]
	hookUserMsg HookUserMsg[T1, US]
}

func ServerUserBase[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](options ...Option[TraceData, TP]) ServerUserOption[T1, TraceData, TP, US] {
	return func(s *serverUserConfig[T1, TraceData, TP, US]) {
		s.options = append(s.options, options...)
	}
}

func ServerUserHookUserMsgOption[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](hook HookUserMsg[T1, US]) ServerUserOption[T1, TraceData, TP, US] {
	return func(s *serverUserConfig[T1, TraceData, TP, US]) {
		s.hookUserMsg = hook
	}
}
