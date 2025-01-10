package service

import (
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/cmap"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/task_group"
)

type BaseService[T any, CTX ctx.IContextPtr[T]] struct {
	h             *handler.Handler[CTX, T]
	natsCluster   *natsclient.ClusterClient
	el            *eventloop.DoubleBuffQueue
	taskGroupHash []task_group.TaskGroup[ce[CTX, T]]
	taskPoolMark  uint64
	taskMap       cmap.ConcurrentMap[uint64, *task_group.TaskGroup[ce[CTX, T]]]
	taskGroupPool *sync.Pool
	taskPool      *task_group.TaskPool
}

type HashRunMode int

const (
	FixedHashPoolMode HashRunMode = 0
	OneHashOneGo      HashRunMode = 1
)

type TaskRunMode int

const (
	// TaskPool all task run in task_group.TaskPool
	TaskPool TaskRunMode = 0
	// OneTaskOneGo one task one goroutine
	OneTaskOneGo TaskRunMode = 1
)

type ce[CTX ctx.IContextPtr[T], T any] struct {
	Data CTX
	Elem *handler.Elem[CTX, T]
}

func NewBaseService[T any, CTX ctx.IContextPtr[T]](
	natsUrls []string,
	lockQueueThread bool,
	hashMode HashRunMode,
	taskMode TaskRunMode,
	rpcTimeout time.Duration,
) *BaseService[T, CTX] {
	s := &BaseService[T, CTX]{
		h:           handler.NewHandler[CTX](),
		natsCluster: natsclient.NewClusterClient(baseenv.GetConfig().ServerType, natsUrls, rpcTimeout),
		el:          eventloop.NewDoubleBuffQueue(lockQueueThread),
		taskMap: cmap.NewWithCustomShardingFunction[uint64, *task_group.TaskGroup[ce[CTX, T]]](
			func(key uint64) uint32 {
				return uint32(key)
			},
		),
		taskGroupPool: objectpool.GetTypePool[task_group.TaskGroup[CTX]](),
	}

	numCpu := uint64(runtime.NumCPU())
	if numCpu&1 == 1 {
		numCpu++
	}
	taskPoolSize := numCpu * 1024
	if hashMode < FixedHashPoolMode || hashMode > OneHashOneGo {
		hashMode = FixedHashPoolMode
	}
	switch hashMode {
	case FixedHashPoolMode:
		numCpu := uint64(runtime.NumCPU())
		if numCpu&1 == 1 {
			numCpu++
		}
		taskPoolSize := numCpu * 1024
		s.taskPoolMark = taskPoolSize - 1
		s.taskGroupHash = make([]task_group.TaskGroup[ce[CTX, T]], taskPoolSize)
		for i := uint64(0); i < taskPoolSize; i++ {
			s.taskGroupHash[i].SetMaxCap(128)
			s.taskGroupHash[i].SetTaskFunc(s.taskFunc)
		}
	}

	if taskMode == TaskPool {
		s.taskPool = task_group.NewTaskPool(int64(taskPoolSize), int64(taskPoolSize*10))
	}

	return s
}

func (s *BaseService[T, CTX]) taskFunc(e task_group.TaskGroupElem[ce[CTX, T]]) {
	defer safego.Recover()
	if e.Data.Data != nil {
		s.handleCtx(e.Data.Data, e.Data.Elem)
	}
	if e.Func != nil {
		e.Func()
	}
}

func (s *BaseService[T, CTX]) GetHandler() *handler.Handler[CTX, T] {
	return s.h
}

func (s *BaseService[T, CTX]) GetNatsCluster() *natsclient.ClusterClient {
	return s.natsCluster
}

func (s *BaseService[T, CTX]) PostEventloop(e any) {
	s.el.PostEventQueue(e)
}

func (s *BaseService[T, CTX]) Start(f func(any)) {
	if f == nil {
		f = func(e any) {
			logger.Log.Warn().Str("type", reflect.TypeOf(e).String()).Any("data", e).Msg("unknown event")
		}
	}
	s.subscribe()
	s.el.Start(
		func(e any) {
			switch c := e.(type) {
			case ce[CTX, T]:
				s.handleCtx(c.Data, c.Elem)
			case func():
				c()
			default:
				if f != nil {
					f(e)
				}
			}
		},
	)
}

func (s *BaseService[T, CTX]) handleCtx(c CTX, e *handler.Elem[CTX, T]) {
	s.call(c, e)
	baseCtx := c.MustBaseContext()
	if baseCtx.Req != nil {
		proto.Reset(baseCtx.Req)
		e.ReqPool().Put(baseCtx.Req)
		baseCtx.Req = nil
	}
	if e.IsRPC() {
		last := len(baseCtx.Resp) - 1
		proto.Reset(baseCtx.Resp[last])
		e.RespPool().Put(baseCtx.Resp[last])
	}
	objectpool.Put[T](c)
}

func (s *BaseService[T, CTX]) call(c CTX, e *handler.Elem[CTX, T]) {
	var err *berror.ErrMsg
	start := time.Now()
	baseCtx := c.MustBaseContext()
	defer func() {
		if err != nil {
			logger.Log.Warn().
				Str("req", e.MsgName()).
				Err(err).
				Dur("cost", time.Since(start)).
				Msg("handle error")
			if baseCtx.NatsMsg != nil {
				err = natsclient.NatsMsgReplyError(baseCtx.NatsMsg, err)
				if err != nil {
					baseCtx.TraceLog.Error().Err(err).Msg("nats reply error")
				}
			}
		}

		baseCtx.TraceLog.Debug().
			Str("req", e.MsgName()).
			Dur("cost", time.Since(start)).
			Msg("handle success")
	}()
	baseCtx.TraceLog.Debug().Str("req", e.MsgName()).Msg("handle start")
	err = e.Call(c)
	if err != nil {
		return
	}
	if baseCtx.NatsMsg != nil && baseCtx.Resp != nil {
		err = natsclient.NatsMsgReply(baseCtx.NatsMsg, baseCtx.Resp...)
		if err != nil {
			return
		}
	}
}

func (s *BaseService[T, CTX]) Stop() {
	s.natsCluster.Close()
	s.el.Stop()
	s.natsCluster.Shutdown()
}

func (s *BaseService[T, CTX]) subscribe() {
	subjInfo := s.h.GetQueueSubjInfo()
	serverId := baseenv.GetConfig().ServerId
	for subj := range subjInfo {
		if serverId == 0 {
			subj = subj + ">"
		} else {
			subj = subj + strconv.FormatInt(serverId, 10) + ".>"
		}

		s.natsCluster.QueueSubscribeAll(subj, s.dealNatsMsg)
		logger.Log.Info().Str("subj", subj).Msg("subscribe queue topic")
	}
	// subscribe broadcast topic
	broadcastSubjInfo := s.h.GetBroadcastSubjInfo()
	for subj := range broadcastSubjInfo {
		// all services of the same type
		subjTop := subj + ">"
		s.natsCluster.SubscribeAll(subjTop, s.dealNatsMsg)
		logger.Log.Info().Str("subjTop", subjTop).Msg("subscribe broadcast top topic")
		// all services of the same type and serverId
		if serverId != 0 {
			subjServerId := subj + strconv.FormatInt(serverId, 10) + ".>"
			s.natsCluster.SubscribeAll(subjServerId, s.dealNatsMsg)
			logger.Log.Info().Str("subjServerId", subjTop).Msg("subscribe broadcast topic")
		}
	}
}

func (s *BaseService[T, CTX]) dealNatsMsg(msg *nats.Msg) {
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

	elem, ok := s.h.GetHandler(msgName)
	if !ok {
		logger.Log.Info().Str("msgName", msgName).Str("subj", msg.Subject).Msg("msg not registered")
		return
	}

	data := msg.Data
	if len(data) < 2 {
		return
	}

	traceSize := int(data[0]) | int(data[1])<<8
	c := objectpool.Get[T]()
	var ca any = c
	ic := ca.(ctx.IContext)
	baseCtx := ic.MustBaseContext()
	if traceSize > 0 {
		traceCtx, ok := ic.(ctx.Trace)
		if ok {
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
			baseCtx.TraceLog.UpdateContext(
				func(c logger.Context) logger.Context {
					return traceCtx.TraceLogField(c.Reset())
				},
			)
		}
	}

	baseCtx.Req = elem.ReqPool().Get().(proto.Message)
	err := proto.Unmarshal(data[2+traceSize:], baseCtx.Req)
	if err != nil {
		if msg.Reply == "" {
			e := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(err))
			if e != nil {
				logger.Log.Error().Err(e).Msg("nats reply error")
			}
		}
		return
	}
	if elem.IsRPC() {
		baseCtx.NatsMsg = msg
	}
	if elem.IsSingle() {
		s.el.PostEventQueue(ce[CTX, T]{Data: c, Elem: elem})
	} else {
		l := len(s.taskGroupHash)
		hash := ic.ToHash()
		if hash != 0 {
			if l > 0 {
				if elem.IsForce() {
					s.taskGroupHash[hash&s.taskPoolMark].PutForce(ce[CTX, T]{Data: c, Elem: elem}, nil)
				} else {
					if !s.taskGroupHash[hash&s.taskPoolMark].Put(ce[CTX, T]{Data: c, Elem: elem}, nil) {
						ReplyTaskPoolFull(baseCtx)
						objectpool.Put[T](c)
						logger.Log.Warn().Err(err).Msg("task group full")
					}
				}

				return
			}
			tg, _ := s.taskMap.GetOrCreate(
				hash, func() *task_group.TaskGroup[ce[CTX, T]] {
					tg := s.taskGroupPool.Get().(*task_group.TaskGroup[ce[CTX, T]])
					tg.SetTaskFunc(s.taskFunc)
					tg.SetMaxCap(128)
					tg.SetOnStop(
						func(t *task_group.TaskGroup[ce[CTX, T]]) {
							tg.SetOnStop(nil)
							s.taskGroupPool.Put(tg)
						},
					)
					return tg
				},
			)
			if elem.IsForce() {
				tg.PutForce(ce[CTX, T]{Data: c, Elem: elem}, nil)
			} else {
				if !tg.Put(ce[CTX, T]{Data: c, Elem: elem}, nil) {
					ReplyTaskPoolFull(baseCtx)
					objectpool.Put[T](c)
					logger.Log.Warn().Err(err).Msg("task group full")
				}
			}
			return
		}
		if s.taskPool != nil {
			if elem.IsForce() {
				s.taskPool.PutForce(
					func() {
						s.handleCtx(c, elem)
					},
				)
			} else {
				if !s.taskPool.Put(
					func() {
						s.handleCtx(c, elem)
					},
				) {
					ReplyTaskPoolFull(baseCtx)
					objectpool.Put[T](c)
					logger.Log.Warn().Err(err).Msg("task group full")
				}
			}
			return
		}
		safego.Go(
			func() {
				defer safego.RecoverWithLogger(baseCtx.TraceLog)
				s.handleCtx(c, elem)
			},
		)
	}
}

func ReplyTaskPoolFull(c *ctx.BaseContext) {
	if c.NatsMsg != nil && c.NatsMsg.Reply != "" {
		err := natsclient.NatsMsgReplyError(c.NatsMsg, berror.NewProtocolStr("task pool full"))
		if err != nil {
			logger.Log.Error().Err(err).Msg("nats reply error")
		}
	}
}
