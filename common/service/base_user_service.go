package service

import (
	"github.com/nats-io/nats.go"
	"github.com/ravinggo/objectpool"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/safego"
)

// ServerUserService is a service,can use user subject.
type ServerUserService[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] struct {
	*BaseService[TraceData, TP]
	userNatsCluster *natsclient.ClusterClientServerUser[T1, US]
	hookUserMsg     HookUserMsg[T1, US]
}

// NewServerUserService create a ServerUserService.
func NewServerUserService[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](
	natsUrls []string,
	ops ...ServerUserOption[T1, TraceData, TP, US],
) *ServerUserService[T1, TraceData, TP, US] {
	serverCnf := &serverUserConfig[T1, TraceData, TP, US]{}
	for _, op := range ops {
		op(serverCnf)
	}
	s := &ServerUserService[T1, TraceData, TP, US]{
		BaseService: NewBaseService[TraceData, TP](
			natsUrls,
			serverCnf.options...,
		),
	}
	if serverCnf.hookUserMsg != nil {
		s.hookUserMsg = serverCnf.hookUserMsg
	}
	s.userNatsCluster = natsclient.NewClusterClientServerUser2[T1, US](
		s.BaseService.natsCluster, s.DealServerUserNatsMsg, serverCnf.unOptions...,
	)
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
	us := (US)(objectpool.Get[T1]())
	defer objectpool.Put[T1](us)
	err := us.ParseSubj(msg.Subject)
	if err != nil {
		return
	}

	data := msg.Data
	if len(data) < 2 {
		return
	}
	traceData, msgData := natsclient.NatsParseUserMsgRaw(data)
	bErr := natsclient.NatsCheckMsg(msgData)
	if bErr != nil {
		return
	}
	if s.hookUserMsg != nil { // hook
		s.doHook(us, traceData, msgData, msg)
		return
	}
	msgCount := 0
	bErr = natsclient.NatsUnmarshalResponseMany(
		msgData, func(msgName string, data []byte) *berror.ErrMsg {
			msgCount++
			elem, ok := s.h.GetHandler(msgName)
			if !ok {
				logger.Log.Info().Str("msgName", msgName).Str("subj", msg.Subject).Msg("msg not registered")
				return nil
			}
			if elem.IsRPC() && msgCount > 1 {
				return berror.NewProtocolStr("invalid rpc request")
			}

			c := objectpool.Get[ctx.BaseCtx[TraceData, TP]]()
			trace := c.GetTrace()
			if len(traceData) > 0 {
				err := trace.TraceMarshalFrom(traceData)
				if err != nil {
					retErr := berror.NewProtocolErr(err)
					if msg.Reply == "" {
						e := natsclient.NatsMsgReplyError(msg, retErr)
						if e != nil {
							logger.Log.Error().Err(e).Msg("nats reply error")
						}
					}
					return retErr
				}
			}
			c.Req = elem.ReqPool().Get().(proto.Message)
			err = define.ProtoUnmarshal(data, c.Req)
			if err != nil {
				retErr := berror.NewProtocolErr(err)
				if msg.Reply == "" {
					e := natsclient.NatsMsgReplyError(msg, retErr)
					if e != nil {
						c.Error().Err(e).Msg("nats reply error")
					}
				}
				return retErr
			}
			if elem.IsRPC() {
				c.NatsMsg = msg
			}

			if elem.IsSingle() {
				s.el.PostEventQueue(ce[TraceData, TP]{Data: c, Elem: elem})
			} else {
				l := len(s.taskGroupHash)
				hash := us.ToHash()
				if hash == 0 && trace != nil {
					hash = trace.ToHash()
				}
				if hash != 0 {
					if l > 0 {
						if elem.IsForce() {
							s.taskGroupHash[hash&s.taskPoolMark].PutForce(ce[TraceData, TP]{Data: c, Elem: elem}, nil)
						} else {
							if !s.taskGroupHash[hash&s.taskPoolMark].Put(ce[TraceData, TP]{Data: c, Elem: elem}, nil) {
								ReplyTaskPoolFull(c)
								c.Warn().Err(err).Msg("task group full")
								s.PutCtxToPool(c)
							}
						}

						return nil
					}

					s.el.PostEventQueue(ce[TraceData, TP]{Data: c, Elem: elem, Hash: hash})
					return nil
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
			return nil
		},
	)
	if bErr != nil {
		logger.Log.Warn().Err(bErr).Str("subj", msg.Subject).Msg("DealServerUserNatsMsg error")
	}
}

func (s *ServerUserService[T1, TraceData, TP, US]) doHook(us US, traceData []byte, data []byte, msg *nats.Msg) {
	defer safego.Recover()
	s.hookUserMsg(us, traceData, data, msg)
}
