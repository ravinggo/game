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

	"github.com/ravinggo/objectpool"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/cmap"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/localevent"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/task_group"
)

type BaseService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	h             *handler.Handler[TraceData, TP]
	natsCluster   *natsclient.ClusterClient
	el            *eventloop.DoubleBuffQueue
	taskGroupHash []task_group.TaskGroup[ce[TraceData, TP]]
	taskPoolMark  uint64
	taskMap       cmap.ConcurrentMap[uint64, *task_group.TaskGroup[ce[TraceData, TP]]]
	taskGroupPool *sync.Pool
	taskPool      *task_group.TaskPool
	serverId      int64
	serverType    string
}

// HashRunMode 0: FixedHashPoolMode, 1: OneHashOneGo
// FixedHashPoolMode use hash to run task,
// OneHashOneGo use one hash one goroutine
type HashRunMode int

const (
	// FixedHashPoolMode fixed((runtime.NumCPU()+1)*1024) task group pool
	// by ToHash() distribute task to task_group.TaskGroup
	FixedHashPoolMode HashRunMode = 0

	// OneHashOneGo one hash one goroutine
	OneHashOneGo HashRunMode = 1
)

type TaskRunMode int

const (
	// TaskPool all task run in task_group.TaskPool
	TaskPool TaskRunMode = 0
	// OneTaskOneGo one task one goroutine
	OneTaskOneGo TaskRunMode = 1
)

type ce[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	Data *ctx.BaseCtx[TraceData, TP]
	Elem *handler.Elem[TraceData, TP]
}

// NewBaseService create a new BaseService
func NewBaseService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	lockQueueThread bool,
	hashMode HashRunMode,
	taskMode TaskRunMode,
	rpcTimeout time.Duration,
	middlewares ...handler.Middleware[TraceData, TP],
) *BaseService[TraceData, TP] {
	s := &BaseService[TraceData, TP]{
		h:           handler.NewHandler[TraceData](middlewares...),
		natsCluster: natsclient.NewClusterClient(natsUrls, nats.Timeout(rpcTimeout)),
		el:          eventloop.NewDoubleBuffQueue(lockQueueThread),
		taskMap: cmap.NewWithCustomShardingFunction[uint64, *task_group.TaskGroup[ce[TraceData, TP]]](
			func(key uint64) uint32 {
				return uint32(key)
			},
		),
		taskGroupPool: objectpool.GetTypePool[task_group.TaskGroup[*ctx.BaseCtx[TraceData, TP]]](),
		serverId:      baseenv.GetConfig().ServerId,
		serverType:    baseenv.GetConfig().ServerType,
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
		s.taskGroupHash = make([]task_group.TaskGroup[ce[TraceData, TP]], taskPoolSize)
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

func (s *BaseService[TraceData, TP]) taskFunc(e task_group.TaskGroupElem[ce[TraceData, TP]]) {
	defer safego.Recover()
	if e.Data.Data != nil {
		s.handleCtx(e.Data.Data, e.Data.Elem)
	}
	if e.Func != nil {
		e.Func()
	}
}

// GetHandler return all registered handlers
func (s *BaseService[TraceData, TP]) GetHandler() *handler.Handler[TraceData, TP] {
	return s.h
}

// GetNatsCluster return nats cluster client
func (s *BaseService[TraceData, TP]) GetNatsCluster() *natsclient.ClusterClient {
	return s.natsCluster
}

// PostEventloop post any event to eventloop
func (s *BaseService[TraceData, TP]) PostEventloop(e any) {
	s.el.PostEventQueue(e)
}

// Start if PostEventloop is being called, param f need to be implemented by the user
func (s *BaseService[TraceData, TP]) Start(f func(any)) {
	if f == nil {
		f = func(e any) {
			logger.Log.Warn().Str("type", reflect.TypeOf(e).String()).Any("data", e).Msg("unknown event")
		}
	}
	s.GetHandler().Logger()
	localevent.Logger()
	s.subscribe()
	s.el.Start(
		func(e any) {
			switch c := e.(type) {
			case ce[TraceData, TP]:
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

func (s *BaseService[TraceData, TP]) handleCtx(c *ctx.BaseCtx[TraceData, TP], e *handler.Elem[TraceData, TP]) {
	s.call(c, e)
	if c.Req != nil {
		e.ReqPool().Put(c.Req)
		c.Req = nil
	}

	objectpool.Put(c)
}

func (s *BaseService[TraceData, TP]) call(c *ctx.BaseCtx[TraceData, TP], e *handler.Elem[TraceData, TP]) {
	var err *berror.ErrMsg
	start := time.Now()
	defer func() {
		if err != nil {
			logger.Log.Warn().
				Str("req", e.MsgName()).
				Err(err).
				Dur("cost", time.Since(start)).
				Msg("handle error")
			if c.NatsMsg != nil {
				errReply := natsclient.NatsMsgReplyError(c.NatsMsg, err)
				if errReply != nil {
					c.Error().Err(errReply).Msg("nats reply error")
					berror.PutErr(errReply)
				}
			}
			berror.PutErr(err)
		}

		c.Debug().
			Str("req", e.MsgName()).
			Dur("cost", time.Since(start)).
			Msg("handle success")
	}()
	c.Debug().Str("req", e.MsgName()).Msg("handle start")
	err = e.Call(c)
	if err != nil {
		return
	}

	if c.NatsMsg != nil && len(c.Resp) != 0 {
		err = natsclient.NatsMsgReply(c.NatsMsg, c.Resp...)
		if e.IsRPCResp() {
			last := len(c.Resp) - 1
			e.RespPool().Put(c.Resp[last])
		}
		if err != nil {
			return
		}
	}
}

// Stop the service
func (s *BaseService[TraceData, TP]) Stop() {
	s.natsCluster.Close()
	s.el.Stop()
	s.natsCluster.Shutdown()
}

func (s *BaseService[TraceData, TP]) subscribe() {
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

func (s *BaseService[TraceData, TP]) dealNatsMsg(msg *nats.Msg) {
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
	c := objectpool.Get[ctx.BaseCtx[TraceData, TP]]()
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

	c.Req = elem.ReqPool().Get().(proto.Message)
	err := proto.Unmarshal(data[2+traceSize:], c.Req)
	if err != nil {
		if msg.Reply == "" {
			e := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(err))
			if e != nil {
				c.Error().Err(e).Msg("nats reply error")
			}
		}
		return
	}
	if elem.IsRPC() {
		c.NatsMsg = msg
	}
	if elem.IsSingle() {
		s.el.PostEventQueue(ce[TraceData, TP]{Data: c, Elem: elem})
	} else {
		l := len(s.taskGroupHash)
		if traceCtx != nil {
			hash := traceCtx.ToHash()
			if hash != 0 {
				if l > 0 {
					if elem.IsForce() {
						s.taskGroupHash[hash&s.taskPoolMark].PutForce(ce[TraceData, TP]{Data: c, Elem: elem}, nil)
					} else {
						if !s.taskGroupHash[hash&s.taskPoolMark].Put(ce[TraceData, TP]{Data: c, Elem: elem}, nil) {
							ReplyTaskPoolFull(c)
							objectpool.Put(c)
							c.Warn().Err(err).Msg("task group full")
						}
					}

					return
				}
				tg, _ := s.taskMap.GetOrCreate(
					hash, func() *task_group.TaskGroup[ce[TraceData, TP]] {
						tg := s.taskGroupPool.Get().(*task_group.TaskGroup[ce[TraceData, TP]])
						tg.SetTaskFunc(s.taskFunc)
						tg.SetMaxCap(128)
						tg.SetOnStop(
							func(t *task_group.TaskGroup[ce[TraceData, TP]]) {
								tg.SetOnStop(nil)
								s.taskGroupPool.Put(tg)
							},
						)
						return tg
					},
				)
				if elem.IsForce() {
					tg.PutForce(ce[TraceData, TP]{Data: c, Elem: elem}, nil)
				} else {
					if !tg.Put(ce[TraceData, TP]{Data: c, Elem: elem}, nil) {
						ReplyTaskPoolFull(c)
						objectpool.Put(c)
						c.Warn().Err(err).Msg("task group full")
					}
				}
				return
			}
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
					ReplyTaskPoolFull(c)
					objectpool.Put(c)
					c.Warn().Err(err).Msg("task group full")
				}
			}
			return
		}
		safego.Go(
			func() {
				defer safego.RecoverWithLogger(c)
				s.handleCtx(c, elem)
			},
		)
	}
}

// ReplyTaskPoolFull reply task pool full error
func ReplyTaskPoolFull[TraceData any, TP ctx.TracePtr[TraceData]](c *ctx.BaseCtx[TraceData, TP]) {
	if c.NatsMsg != nil && c.NatsMsg.Reply != "" {
		err := natsclient.NatsMsgReplyError(c.NatsMsg, berror.NewProtocolStr("task pool full"))
		if err != nil {
			logger.Log.Error().Err(err).Msg("nats reply error")
		}
	}
}
