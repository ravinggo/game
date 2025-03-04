package service

import (
	"strings"

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
type ServerUserService[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] struct {
	*BaseService[TraceData, TP]
	userNatsCluster *natsclient.ClusterClientServerUser[T1, US]
}

// NewServerUserService create a ServerUserService.
func NewServerUserService[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](
	natsUrls []string,
	ops ...Option[TraceData, TP],
) *ServerUserService[T1, TraceData, TP, US] {
	s := &ServerUserService[T1, TraceData, TP, US]{
		BaseService: NewBaseService[TraceData, TP](
			natsUrls,
			ops...,
		),
	}
	s.userNatsCluster = natsclient.NewClusterClientServerUser2[T1, US](s.BaseService.natsCluster, s.DealServerUserNatsMsg)
	return s
}

// GetUserNatsCluster return *ClusterClientServerUser.
func (s *ServerUserService[T1, TraceData, TP, US]) GetUserNatsCluster() *natsclient.ClusterClientServerUser[T1, US] {
	return s.userNatsCluster
}

// UserSubscribeOne subscribe one user subject.
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeOne(us US) {
	s.userNatsCluster.UserSubscribeOne(us)
}

// UserSubscribeOneWaitSuccess subscribe one user subject and wait success.
// Only used in multi-cluster or multiple connections to the same cluster
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeOneWaitSuccess(us US) {
	s.userNatsCluster.UserSubscribeOneWaitSuccess(us)
}

// UserSubscribeAll subscribe all NatsClient for user subject.
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeAll(us US) {
	s.userNatsCluster.UserSubscribeAll(us)
}

// UserSubscribeAllWaitSuccess subscribe all NatsClient for user subject and wait success.
// Only used in multi-cluster or multiple connections to the same cluster
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeAllWaitSuccess(us US) {
	s.userNatsCluster.UserSubscribeAllWaitSuccess(us)
}

// UserUnsubscribe unsubscribe user subject if subscribed.
func (s *ServerUserService[T1, TraceData, TP, US]) UserUnsubscribe(us US) {
	s.userNatsCluster.UserUnsubscribe(us)
}

func (s *ServerUserService[T1, TraceData, TP, US]) DealServerUserNatsMsg(msg *nats.Msg) {
	index := strings.IndexByte(msg.Subject, '.')
	if index == -1 {
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
	err = define.ProtoUnmarshal(data[2+traceSize:], c.Req)
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
		hash := us.ToHash()
		if hash == 0 && traceCtx != nil {
			hash = traceCtx.ToHash()
		}
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
		} else {
			c.Error().Err(define.ErrInvalidToHash).Msg("DealServerUserNatsMsg not dispatch")
			if msg.Reply != "" { // RPC
				err := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(define.ErrInvalidToHash))
				if err != nil {
					c.Error().Err(err).Msg("nats reply error")
				}
			}
		}
	}
}
