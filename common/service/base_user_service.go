package service

import (
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/task_group"
)

// ServerUserService is a service,can use user subject.
type ServerUserService[T1 any, T any, CTX ctx.IContextPtr[T], US natsclient.ServerUserSubjectPtr[T1]] struct {
	*BaseService[T, CTX]
	userNatsCluster *natsclient.ClusterClientServerUser[T1, US]
}

// NewServerUserService create a ServerUserService.
func NewServerUserService[T1 any, T any, CTX ctx.IContextPtr[T], US natsclient.ServerUserSubjectPtr[T1]](
	natsUrls []string,
	lockQueueThread bool,
	hashMode HashRunMode,
	taskMode TaskRunMode,
	rpcTimeout time.Duration,
) *ServerUserService[T1, T, CTX, US] {
	s := &ServerUserService[T1, T, CTX, US]{
		BaseService: NewBaseService[T, CTX](
			natsUrls,
			lockQueueThread,
			hashMode,
			taskMode,
			rpcTimeout,
		),
	}
	s.userNatsCluster = natsclient.NewClusterClientServerUser2[T1, US](s.BaseService.natsCluster)
	return s
}

// GetUserNatsCluster return *ClusterClientServerUser.
func (s *ServerUserService[T1, T, CTX, US]) GetUserNatsCluster() *natsclient.ClusterClientServerUser[T1, US] {
	return s.userNatsCluster
}

// UserSubscribeOne subscribe one user subject.
func (s *ServerUserService[T1, T, CTX, US]) UserSubscribeOne(us US) {
	s.userNatsCluster.UserSubscribeOne(us, s.dealServerUserNatsMsg)
}

// UserSubscribeOneWaitSuccess subscribe one user subject and wait success.
// Only used in multi-cluster or multiple connections to the same cluster
func (s *ServerUserService[T1, T, CTX, US]) UserSubscribeOneWaitSuccess(us US) {
	s.userNatsCluster.UserSubscribeOneWaitSuccess(us, s.dealServerUserNatsMsg)
}

// UserSubscribeAll subscribe all NatsClient for user subject.
func (s *ServerUserService[T1, T, CTX, US]) UserSubscribeAll(us US) {
	s.userNatsCluster.UserSubscribeAll(us, s.dealServerUserNatsMsg)
}

// UserSubscribeAllWaitSuccess subscribe all NatsClient for user subject and wait success.
// Only used in multi-cluster or multiple connections to the same cluster
func (s *ServerUserService[T1, T, CTX, US]) UserSubscribeAllWaitSuccess(us US) {
	s.userNatsCluster.UserSubscribeAllWaitSuccess(us, s.dealServerUserNatsMsg)
}

// UserUnsubscribe unsubscribe user subject if subscribed.
func (s *ServerUserService[T1, T, CTX, US]) UserUnsubscribe(us US) {
	s.userNatsCluster.UserUnsubscribe(us)
}

func (s *ServerUserService[T1, T, CTX, US]) dealServerUserNatsMsg(msg *nats.Msg) {
	index := strings.IndexByte(msg.Subject, '.')
	if index == -1 {
		return
	}
	if msg.Subject[index+1] == '>' && msg.Reply != "" && len(msg.Data) == 0 {
		err := msg.Respond(nil)
		if err != nil {
			logger.Log.Error().Err(err).Str("msgName", msg.Subject).Msg("deal wait success error")
		}
		return
	}
	msgName := msg.Subject[index+1:]
	elem, ok := s.h.GetHandler(msgName)
	if !ok {
		logger.Log.Info().Str("msgName", msgName).Str("subj", msg.Subject).Msg("msg not registered")
		return
	}
	us := (US)(objectpool.Get[T1]())
	defer objectpool.Put[T1](us)
	err := us.ParseSubjForCall(msg.Subject[:index])
	if err != nil {
		return
	}

	data := msg.Data
	if len(data) < 2 {
		return
	}

	traceSize := int(data[0]) | int(data[1])<<8
	c := (CTX)(objectpool.Get[T]())
	baseCtx := c.MustBaseContext()
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
		baseCtx.TraceLog.UpdateContext(
			func(c logger.Context) logger.Context {
				return traceCtx.TraceLogField(c.Reset())
			},
		)
	}

	baseCtx.Req = elem.ReqPool().Get().(proto.Message)
	err = proto.Unmarshal(data[2+traceSize:], baseCtx.Req)
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
		hash := us.ToHash()
		if hash == 0 && traceCtx != nil {
			hash = traceCtx.ToHash()
		}
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
		} else {
			logger.Log.Error().Err(define.ErrInvalidToHash).Msg("dealServerUserNatsMsg not dispatch")
			if msg.Reply != "" { // RPC
				err := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(define.ErrInvalidToHash))
				if err != nil {
					logger.Log.Error().Err(err).Msg("nats reply error")
				}
			}
		}
	}
}
