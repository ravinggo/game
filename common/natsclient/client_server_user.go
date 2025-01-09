package natsclient

import (
	"hash/crc64"
	"math"
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/utils"
)

// ServerUserSubject one user on one server type subscribe
// all message for this user deal by this subject
// example: subscribe "{ServerType}/{ServerId}/{RoleId}.>", publish "{serverType}/{serverId}/{roleId}.{MsgName}"
// because when NATS reconnects, it will send all the subjects subscribed to the current connection to NATS-Server for re-subscription.
// too many subjects will cause the reconnection to be too slow.
// so it is ideal for one user to subscribe to the same service once.
type ServerUserSubject interface {
	// ToHash for switch one NatsClient
	ToHash() uint64

	// CreateSubj create subj for NatsClient.SubscribeServerUser
	// param b from objectpool.GetBytes
	CreateSubj(*objectpool.Bytes)

	// CreateSubjForCallSize calculate size of CreateSubjForCall
	CreateSubjForCallSize() int

	// CreateSubjForCall create subj for NatsClient.PublishServerUser and NatsClient.RequestServerUser
	CreateSubjForCall(*objectpool.Bytes)
}

type ServerUserSubjectPtr[T any] interface {
	ServerUserSubject
	*T
}

type ServerIntUserSubject struct {
	ServerType string
	ServerId   int64
	RoleId     int64
	MsgName    string
}

func (u *ServerIntUserSubject) CreateSubj(b *objectpool.Bytes) {
	b.WriteString(u.ServerType)
	b.WriteBytes('/')
	b.WriteInt(u.ServerId)
	b.WriteBytes('/')
	b.WriteInt(u.RoleId)
	b.WriteString(".>")
}
func (u *ServerIntUserSubject) ToHash() uint64 {
	return uint64(u.RoleId)
}

func (u *ServerIntUserSubject) CreatePublishSize() int {
	return len(u.ServerType) + utils.CountIntByte(u.ServerId) + utils.CountIntByte(u.RoleId) + len(u.MsgName) + 3
}

func (u *ServerIntUserSubject) CreateSubjForCall(bytes *objectpool.Bytes) {
	bytes.Reset()
	bytes.WriteString(u.ServerType)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.ServerId)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.RoleId)
	bytes.WriteBytes('.')
	bytes.WriteString(u.MsgName)
}

type ServerStringUserSubject struct {
	ServerType string
	ServerId   int64
	RoleId     string
	MsgName    string
}

func (u *ServerStringUserSubject) CreateSubj(b *objectpool.Bytes) {
	b.WriteString(u.ServerType)
	b.WriteBytes('/')
	b.WriteInt(u.ServerId)
	b.WriteBytes('/')
	b.WriteString(u.RoleId)
	b.WriteString(".>")
}

func (u *ServerStringUserSubject) ToHash() uint64 {
	return crc64.Checksum(utils.StringToBytes(u.RoleId), crc64.MakeTable(crc64.ECMA))
}

func (u *ServerStringUserSubject) CreatePublishSize() int {
	return len(u.ServerType) + utils.CountIntByte(u.ServerId) + len(u.RoleId) + len(u.MsgName) + 3
}

func (u *ServerStringUserSubject) CreatePublish(bytes *objectpool.Bytes) {
	bytes.Reset()
	bytes.WriteString(u.ServerType)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.ServerId)
	bytes.WriteBytes('/')
	bytes.WriteString(u.RoleId)
	bytes.WriteBytes('.')
	bytes.WriteString(u.MsgName)
}

// ClientSubscribeServerUser Generic Implementation : subscribe user topic
// param us not escapes to heap
func ClientSubscribeServerUser[US ServerUserSubjectPtr[T], T any](nc *NatsClient, us US, handler nats.MsgHandler) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if _, ok := nc.subs.Get(subj); ok {
		return false
	}
	sub, err := nc.conn.Subscribe(subj, handler)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Msg("ClientSubscribeServerUser")
	}
	if !nc.subs.SetIfAbsent(subj, sub) {
		err := sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Msg("ClientSubscribeServerUser for Unsubscribe")
		}
		return false
	}
	logger.Log.Info().Str("subj", subj).Msg("ClientSubscribeServerUser")
	return true
}

// ClientQueueSubscribeServerUser Generic Implementation : queue subscribe user topic
// param us not escapes to heap
func ClientQueueSubscribeServerUser[US ServerUserSubjectPtr[T], T any](nc *NatsClient, us US, handler nats.MsgHandler) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if _, ok := nc.subs.Get(subj); ok {
		return false
	}
	group := subj[:len(subj)-2]
	sub, err := nc.conn.QueueSubscribe(subj, group, handler)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Str("group", group).Msg("ClientQueueSubscribeServerUser")
	}
	if !nc.subs.SetIfAbsent(subj, sub) {
		err := sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Str("group", group).Msg("QueueSubscribeServerUser for Unsubscribe")
		}
		return false
	}
	logger.Log.Info().Str("subj", subj).Str("group", group).Msg("ClientQueueSubscribeServerUser")
	return true
}

// ClientUnsubscribeServerUser Generic Implementation : unsubscribe user topic
// param us not escapes to heap
func ClientUnsubscribeServerUser[US ServerUserSubjectPtr[T], T any](nc *NatsClient, us US) {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if v, ok := nc.subs.GetAndRemove(subj); ok {
		safego.Go(
			func() {
				if v.IsValid() {
					err := v.Drain()
					if err != nil {
						logger.Log.Error().Err(err).Str("subj", subj).Msg("Client[Un]subscribeUser Drain error")
					}
				}
				for i := 0; i < 10; i++ {
					if !v.IsValid() {
						break
					}
				}
			},
		)
		logger.Log.Info().Str("subj", subj).Msg("Client[Un]subscribeUser")
	}
}

// SubscribeServerUser Subscribe user topic
// param us escapes to heap
// recommended use ClientSubscribeServerUser
func (this_ *NatsClient) SubscribeServerUser(us ServerUserSubject, handler nats.MsgHandler) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if _, ok := this_.subs.Get(subj); ok {
		return false
	}
	sub, err := this_.conn.Subscribe(subj, handler)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Msg("SubscribeServerUser")
	}
	if !this_.subs.SetIfAbsent(subj, sub) {
		err := sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Msg("SubscribeServerUser for Unsubscribe")
		}
		return false
	}
	logger.Log.Info().Str("subj", subj).Msg("SubscribeServerUser")
	return true
}

// QueueSubscribeServerUser QueueSubscribe user topic
// param us escapes to heap
// recommended use ClientQueueSubscribeServerUser
func (this_ *NatsClient) QueueSubscribeServerUser(us ServerUserSubject, handler nats.MsgHandler) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if _, ok := this_.subs.Get(subj); ok {
		return false
	}
	group := subj[:len(subj)-2]
	sub, err := this_.conn.QueueSubscribe(subj, group, handler)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Str("group", group).Msg("QueueSubscribeServerUser")
	}
	if !this_.subs.SetIfAbsent(subj, sub) {
		err := sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Str("group", group).Msg("QueueSubscribeServerUser for Unsubscribe")
		}
		return false
	}
	logger.Log.Info().Str("subj", subj).Str("group", group).Msg("QueueSubscribeServerUser")
	return true
}

// UnsubscribeServerUser Unsubscribe user topic
// param us escapes to heap
// recommended use ClientUnsubscribeServerUser
func (this_ *NatsClient) UnsubscribeServerUser(us ServerUserSubject) {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if v, ok := this_.subs.GetAndRemove(subj); ok {
		safego.Go(
			func() {
				if v.IsValid() {
					err := v.Drain()
					if err != nil {
						logger.Log.Error().Err(err).Str("subj", subj).Msg("[Un]subscribeUser Drain error")
					}
				}
				for i := 0; i < 10; i++ {
					if !v.IsValid() {
						break
					}
				}
			},
		)
		logger.Log.Info().Str("subj", subj).Msg("Queue[Un]subscribeUser")
	}
}

// PublishServerUser Generic Implementation : publish user topic
// param us,pubMsg escapes to heap
// recommended use ClientPublishServerUser
func (this_ *NatsClient) PublishServerUser(c ctx.IContext, us ServerUserSubject, pubMsg proto.Message) *berror.ErrMsg {
	traceSize := 0
	var err error
	var traceCtx ctx.Trace
	if c != nil {
		var ok bool
		traceCtx, ok = c.(ctx.Trace)
		if ok {
			oldServerId, oldServerType := traceCtx.GetServerIdAndType()
			traceCtx.SetServerIdAndType(baseenv.GetConfig().ServerId, baseenv.GetConfig().ServerType)
			defer traceCtx.SetServerIdAndType(oldServerId, oldServerType)
			traceSize = traceCtx.TraceMarshalSize()
		}
	}
	if traceSize > math.MaxUint16 {
		return berror.NewProtocolStr("trace data too long,max size is 65535")
	}
	size := us.CreateSubjForCallSize()
	msgSize := proto.Size(pubMsg)
	b := objectpool.GetBytes(size + 2 + traceSize + msgSize)
	defer objectpool.PutBytes(b)
	us.CreateSubjForCall(b)
	if traceSize > 0 {
		b.Data = append(b.Data, byte(traceSize), byte(traceSize>>8))
		b.Data, err = traceCtx.TraceMarshalAppend(b.Data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		b.Data = append(b.Data, 0, 0)
	}
	_, err = proto.MarshalOptions{}.MarshalAppend(b.Data, pubMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}

	err = this_.conn.Publish(utils.BytesToString(b.Data[:size]), b.Data[size:])
	return berror.NewProtocolErr(err)
}

// RequestServerUser user topic rpc
// us,reqMsg,out escapes to heap
// recommended use ClientRequestServerUser
func (this_ *NatsClient) RequestServerUser(c ctx.IContext, us ServerUserSubject, reqMsg proto.Message, out proto.Message) *berror.ErrMsg {
	traceSize := 0
	var err error
	var traceCtx ctx.Trace
	if c != nil {
		var ok bool
		traceCtx, ok = c.(ctx.Trace)
		if ok {
			oldServerId, oldServerType := traceCtx.GetServerIdAndType()
			traceCtx.SetServerIdAndType(baseenv.GetConfig().ServerId, baseenv.GetConfig().ServerType)
			defer traceCtx.SetServerIdAndType(oldServerId, oldServerType)
			traceSize = traceCtx.TraceMarshalSize()
		}
	}
	if traceSize > math.MaxUint16 {
		return berror.NewProtocolStr("trace data too long,max size is 65535")
	}

	size := us.CreateSubjForCallSize()
	msgSize := proto.Size(reqMsg)
	b := objectpool.GetBytes(size + 2 + traceSize + msgSize)
	defer objectpool.PutBytes(b)
	us.CreateSubjForCall(b)
	if traceSize > 0 {
		b.Data = append(b.Data, byte(traceSize), byte(traceSize>>8))
		b.Data, err = traceCtx.TraceMarshalAppend(b.Data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		b.Data = append(b.Data, 0, 0)
	}
	_, err = proto.MarshalOptions{}.MarshalAppend(b.Data, reqMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	outMsg, err := this_.conn.Request(utils.BytesToString(b.Data[:size]), b.Data[size:], this_.timeout)
	if err != nil {
		return berror.NewProtocolErr(err)
	}

	return NatsUnmarshalResponseWithout(outMsg.Data, out)
}

// ClientPublishServerUser Generic Implementation : publish user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see NatsClient.PublishServerUser
// create use NewClientPublishServerUser
type ClientPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]] struct {
	Pub    T1
	Us     T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClientPublishServerUser[T, T1, US, PUB]) Reset() {
	*r = ClientPublishServerUser[T, T1, US, PUB]{}
}

// NewClientPublishServerUser create ClientPublishServerUser for objectpool
func NewClientPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]]() *ClientPublishServerUser[T, T1, US, PUB] {
	c := objectpool.Get[ClientPublishServerUser[T, T1, US, PUB]]()
	c.forNew = 1
	return c
}

// Publish more info see NatsClient.PublishServerUser
func (r *ClientPublishServerUser[T, T1, US, PUB]) Publish(nc *NatsClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClientPublishServerUser please use NewClientPublishServerUser")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return nc.PublishServerUser(c, (US)(&r.Us), (PUB)(&r.Pub))
	}
	panic("ClientPublishServerUser used")
}

// ClientRequestServerUser Generic Implementation : rpc user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see NatsClient.RequestServerUser
// create use NewClientRequestServerUser
type ClientRequestServerUser[T, T1, T2 any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]] struct {
	Req    T1
	Resp   T2
	Us     T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClientRequestServerUser[T, T1, T2, US, REQ, RESP]) Reset() {
	*r = ClientRequestServerUser[T, T1, T2, US, REQ, RESP]{}
}

// NewClientRequestServerUser create ClientRequestServerUser for objectpool
func NewClientRequestServerUser[T, T1, T2 any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]](
) *ClientRequestServerUser[T, T1, T2, US, REQ, RESP] {
	c := objectpool.Get[ClientRequestServerUser[T, T1, T2, US, REQ, RESP]]()
	c.forNew = 1
	return c
}

// Request more info see NatsClient.Request
func (r *ClientRequestServerUser[T, T1, T2, US, REQ, RESP]) Request(nc *NatsClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClientRequestServerUser please use NewClientRequestServerUser")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return nc.RequestServerUser(c, (US)(&r.Us), (REQ)(&r.Req), (RESP)(&r.Resp))
	}
	panic("ClientRequestServerUser used")
}
