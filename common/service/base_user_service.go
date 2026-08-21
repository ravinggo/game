package service

import (
	"github.com/nats-io/nats.go"
	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/safego"
)

// ServerUserService extends OneHashOneGoService with per-user NATS subject subscriptions.
// User messages are always hash-routed by user ID, making OneHashOneGo the natural fit.
// Written by Claude Code claude-opus-4-6.
type ServerUserService[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]] struct {
	*OneHashOneGoService[TraceData, TP]
	userNatsCluster *natsclient.ClusterClientServerUser[T1, US]
	hookUserMsg     HookUserMsg[T1, US]
}

// NewServerUserService creates a ServerUserService backed by OneHashOneGo dispatch.
// Written by Claude Code claude-opus-4-6.
func NewServerUserService[T1 any, TraceData any, TP ctx.TracePtr[TraceData], US natsclient.ServerUserSubjectPtr[T1]](
	natsUrls []string,
	c ServerUserConfig[T1, TraceData, TP, US],
) *ServerUserService[T1, TraceData, TP, US] {
	serverCnf := &ServerUserConfig[T1, TraceData, TP, US]{}
	s := &ServerUserService[T1, TraceData, TP, US]{
		OneHashOneGoService: NewOneHashOneGoService[TraceData, TP](
			natsUrls, c.Config,
		),
	}
	if serverCnf.HookUserMsg != nil {
		s.hookUserMsg = serverCnf.HookUserMsg
	}
	s.userNatsCluster = natsclient.NewClusterClientServerUser2[T1, US](
		s.BaseService.natsCluster, s.DealServerUserNatsMsg, serverCnf.UNOptions...,
	)
	return s
}

// GetUserNatsCluster returns the per-user NATS cluster client.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) GetUserNatsCluster() *natsclient.ClusterClientServerUser[T1, US] {
	return s.userNatsCluster
}

// UserSubscribeOne subscribes to one user subject on a single NATS connection.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeOne(us US) {
	s.userNatsCluster.UserSubscribeOne(us)
}

// UserSubscribeOneWaitSuccess subscribes to one user subject and waits for confirmation.
// Only useful with multi-cluster or multiple connections to the same cluster.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeOneWaitSuccess(us US) {
	s.userNatsCluster.UserSubscribeOneWaitSuccess(us)
}

// UserSubscribeAll subscribes to a user subject on all NATS connections.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeAll(us US) {
	s.userNatsCluster.UserSubscribeAll(us)
}

// UserSubscribeAllWaitSuccess subscribes to a user subject on all connections and waits.
// Only useful with multi-cluster or multiple connections to the same cluster.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) UserSubscribeAllWaitSuccess(us US) {
	s.userNatsCluster.UserSubscribeAllWaitSuccess(us)
}

// UserUnsubscribe removes the subscription for a user subject if present.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) UserUnsubscribe(us US) {
	s.userNatsCluster.UserUnsubscribe(us)
}

// DealServerUserNatsMsg is the NATS callback for per-user subject messages. It parses
// the user subject, validates the wire payload, and dispatches each embedded proto
// message through the handler registry with user-hash-based routing.
// Written by Claude Code claude-opus-4-6.
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
	if s.hookUserMsg != nil {
		if s.doHook(us, traceData, msgData, msg) {
			return
		}
	}
	msgCount := 0
	bErr = natsclient.NatsUnmarshalResponseMany(
		msgData, func(msgName string, data []byte) *berror.ErrMsg {
			msgCount++
			elem, ok := s.GetHandler().Lookup(msgName)
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
					if msg.Reply != "" {
						e := natsclient.NatsMsgReplyError(msg, retErr)
						if e != nil {
							logger.Log.Error().Err(e).Msg("nats reply error")
						}
					}
					return retErr
				}
			}
			c.Req, c.Resp = elem.Acquire()
			if elem.IsRPC() {
				c.NatsMsg = msg
			}
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

			roleID := us.GetRoleID()
			if roleID == 0 && trace != nil {
				roleID = trace.GetRoleID()
			}
			if roleID == 0 {
				c.Error().Err(define.ErrZeroRoleID).Msg("DealServerUserNatsMsg: zero RoleID for non-single handler")
				if msg.Reply != "" {
					e := natsclient.NatsMsgReplyError(msg, berror.NewProtocolErr(define.ErrZeroRoleID))
					if e != nil {
						c.Error().Err(e).Msg("nats reply error")
					}
				}
				return nil
			}
			c.GetTrace().SetRoleID(roleID)
			s.dispatch(c, elem)
			return nil
		},
	)
	if bErr != nil {
		logger.Log.Warn().Err(bErr).Str("subj", msg.Subject).Msg("DealServerUserNatsMsg error")
	}
}

// doHook calls the optional hookUserMsg callback with panic recovery so that a bad
// hook cannot crash the NATS subscription goroutine.
// Written by Claude Code claude-opus-4-6.
func (s *ServerUserService[T1, TraceData, TP, US]) doHook(us US, traceData []byte, data []byte, msg *nats.Msg) bool {
	defer safego.Recover()
	return s.hookUserMsg(us, traceData, data, msg)
}
