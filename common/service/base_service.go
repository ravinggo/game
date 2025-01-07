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

type handlerElemKey struct{}

type BaseService[CTX ctx.IContext] struct {
	h             *handler.Handler[CTX]
	natsCluster   *natsclient.ClusterClient
	el            *eventloop.DoubleBuffQueue
	taskGroupHash []task_group.TaskGroup[CTX]
	taskPoolMark  uint64
	taskMap       cmap.ConcurrentMap[uint64, *task_group.TaskGroup[CTX]]
	taskGroupPool *sync.Pool
	taskPool      *task_group.TaskPool
}

type RunMode int

const (
	FixedGoPoolMode RunMode = iota
	OneHashOneGo
	OneTaskOneGo
)

func NewBaseService[CTX ctx.IContext](
	natsUrls []string,
	lockQueueThread bool,
	mode RunMode,
	rpcTimeout time.Duration,
) *BaseService[CTX] {
	s := &BaseService[CTX]{
		h:           handler.NewHandler[CTX](),
		natsCluster: natsclient.NewClusterClient(baseenv.GetConfig().ServerType, natsUrls, rpcTimeout),
		el:          eventloop.NewDoubleBuffQueue(lockQueueThread),
		taskMap: cmap.NewWithCustomShardingFunction[uint64, *task_group.TaskGroup[CTX]](
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
	if mode < FixedGoPoolMode || mode > OneTaskOneGo {
		mode = FixedGoPoolMode
	}
	switch mode {
	case FixedGoPoolMode:
		numCpu := uint64(runtime.NumCPU())
		if numCpu&1 == 1 {
			numCpu++
		}
		taskPoolSize := numCpu * 1024
		s.taskPoolMark = taskPoolSize - 1
		s.taskGroupHash = make([]task_group.TaskGroup[CTX], taskPoolSize)
		for i := uint64(0); i < taskPoolSize; i++ {
			s.taskGroupHash[i].SetMaxCap(128)
			s.taskGroupHash[i].SetTaskFunc(
				func(e task_group.TaskGroupElem[CTX]) {
					defer safego.Recover()
					if e.Data != nil {
						c := e.Data
						s.handleCtx(c)
					}
					if e.Func != nil {
						e.Func()
					}
				},
			)
		}
	}
	s.taskPool = task_group.NewTaskPool(int64(taskPoolSize), int64(taskPoolSize*10))
	return s
}

func (s *BaseService[CTX]) GetHandler() *handler.Handler[CTX] {
	return s.h
}

func (s *BaseService[CTX]) GetNatsCluster() *natsclient.ClusterClient {
	return s.natsCluster
}

func (s *BaseService[CTX]) PostEventloop(e any) {
	s.el.PostEventQueue(e)
}

func (s *BaseService[CTX]) Start(f func(any)) {
	if f == nil {
		f = func(e any) {
			logger.Log.Warn().Str("type", reflect.TypeOf(e).String()).Any("data", e).Msg("unknown event")
		}
	}
	s.el.Start(
		func(e any) {
			switch c := e.(type) {
			case CTX:
				s.handleCtx(c)
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

func (s *BaseService[CTX]) handleCtx(c CTX) {
	e, ok := ctx.Value[handlerElemKey, *handler.Elem[CTX]](c, handlerElemKey{})
	if ok {
		s.call(c, e)
	} else {
		logger.Log.Warn().Msgf("invalid %s,ctx not found handler elem", reflect.TypeOf(c).String())
	}
}

func (s *BaseService[CTX]) call(c CTX, e *handler.Elem[CTX]) {
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

func (s *BaseService[CTX]) Stop() {
	s.el.Stop()
	s.natsCluster.Shutdown()
}

func (s *BaseService[CTX]) subscribe() {
	subjInfo := s.h.GetQueueSubjInfo()
	serverId := baseenv.GetConfig().ServerId
	for subj := range subjInfo {
		if serverId == 0 {
			subj = subj + ".>"
		} else {
			subj = subj + "." + strconv.FormatInt(serverId, 10) + ".>"
		}

		s.natsCluster.QueueSubscribeAll(subj, s.dealNatsMsg)
		logger.Log.Info().Str("subj", subj).Msg("subscribe queue topic")
	}
	// subscribe broadcast topic
	broadcastSubjInfo := s.h.GetBroadcastSubjInfo()
	for subj := range broadcastSubjInfo {
		// all services of the same type
		subjTop := subj + ".>"
		s.natsCluster.SubscribeAll(subjTop, s.dealNatsMsg)
		logger.Log.Info().Str("subjTop", subjTop).Msg("subscribe broadcast top topic")
		// all services of the same type and serverId
		if serverId != 0 {
			subjServerId := subj + "." + strconv.FormatInt(serverId, 10) + ".>"
			s.natsCluster.SubscribeAll(subjServerId, s.dealNatsMsg)
			logger.Log.Info().Str("subjServerId", subjTop).Msg("subscribe broadcast topic")
		}
	}
}

func (s *BaseService[CTX]) dealNatsMsg(msg *nats.Msg) {
	msgName := msg.Subject
	index := strings.LastIndexByte(msgName, '.')
	if index == -1 {
		return
	}
	msgName = msgName[index+1:]
	data := msg.Data
	if len(data) < 2 {
		return
	}

	traceSize := int(data[0]) | int(data[1])<<8
	c := objectpool.GetElem[CTX]()
	var ic ctx.IContext = c
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
			baseCtx.TraceLog.UpdateContext(traceCtx.TraceLogField)
		}
	}

	elem, ok := s.h.GetHandler(msgName)
	if !ok {
		logger.Log.Info().Str("msgName", msgName).Str("subj", msg.Subject).Msg("msg not registered")
		return
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
	baseCtx.SetValue(handlerElemKey{}, elem)
	if elem.IsSingle() {
		s.PostEventloop(ic)
	} else {
		l := len(s.taskGroupHash)
		hash := ic.ToHash()
		if hash != 0 {
			if l > 0 {
				if elem.IsForce() {
					s.taskGroupHash[hash&s.taskPoolMark].PutForce(c, nil)
				} else {
					if !s.taskGroupHash[hash&s.taskPoolMark].Put(c, nil) {
						ReplyTaskPoolFull(baseCtx)
						logger.Log.Warn().Err(err).Msg("task group full")
					}
				}

				return
			}
			tg, _ := s.taskMap.GetOrCreate(
				hash, func() *task_group.TaskGroup[CTX] {
					tg := s.taskGroupPool.Get().(*task_group.TaskGroup[CTX])
					tg.SetTaskFunc(
						func(e task_group.TaskGroupElem[CTX]) {
							defer safego.RecoverWithLogger(baseCtx.TraceLog)
							if e.Data != nil {
								s.handleCtx(e.Data)
							}
							if e.Func != nil {
								e.Func()
							}
						},
					)
					tg.SetMaxCap(128)
					tg.SetOnStop(
						func(t *task_group.TaskGroup[CTX]) {
							tg.SetOnStop(nil)
							s.taskGroupPool.Put(tg)
						},
					)
					return tg
				},
			)
			if elem.IsForce() {
				tg.PutForce(c, nil)
			} else {
				if !tg.Put(c, nil) {
					ReplyTaskPoolFull(baseCtx)
					logger.Log.Warn().Err(err).Msg("task group full")
				}
			}
			return
		}
		if s.taskPool != nil {
			if elem.IsForce() {
				s.taskPool.PutForce(
					func() {
						s.handleCtx(c)
					},
				)
			} else {
				if s.taskPool.Put(
					func() {
						s.handleCtx(c)
					},
				) {
					ReplyTaskPoolFull(baseCtx)
					logger.Log.Warn().Err(err).Msg("task group full")
				}
			}
			return
		}
		safego.Go(
			func() {
				defer safego.RecoverWithLogger(baseCtx.TraceLog)
				s.handleCtx(c)
			},
		)
	}
}

func ReplyTaskPoolFull(ctx *ctx.BaseContext) {
	if ctx.NatsMsg != nil && ctx.NatsMsg.Reply != "" {
		err := natsclient.NatsMsgReplyError(ctx.NatsMsg, berror.NewProtocolStr("task pool full"))
		if err != nil {
			logger.Log.Error().Err(err).Msg("nats reply error")
		}
	}
}
