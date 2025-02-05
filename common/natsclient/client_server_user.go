package natsclient

import (
	"errors"
	"hash/crc64"
	"math"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/objectpool"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
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
	// ToHash for switch one NatsClient And Dispatch call,must not 0
	ToHash() uint64

	// CreateSubj create subj for NatsClient.SubscribeServerUser
	// param b from objectpool.GetBytes
	CreateSubj(*objectpool.Bytes)

	// CreateSubjForCallSize calculate size of CreateSubjForCall
	CreateSubjForCallSize() int

	// CreateSubjForCall create subj for NatsClient.PublishServerUser and NatsClient.RequestUser
	CreateSubjForCall(*objectpool.Bytes)

	// ParseSubjForCall parse for subj
	ParseSubjForCall(s string) error
}

type ServerUserSubjectPtr[T any] interface {
	ServerUserSubject
	*T
}

type ServerUserNatsClient[T any, US ServerUserSubjectPtr[T]] struct {
	*NatsClient
	queues         []chan *nats.Msg
	userHandler    nats.MsgHandler
	isCreateQueues bool
	closeCh        chan struct{}
}

func NewServerUserNatsClient[T any, US ServerUserSubjectPtr[T]](
	urls string, userHandler nats.MsgHandler, opts ...UNOption,
) *ServerUserNatsClient[T, US] {
	if userHandler == nil {
		panic("handler is nil")
	}
	uh := func(msg *nats.Msg) {
		defer safego.Recover()
		if strings.HasSuffix(msg.Subject, waitSuccessCheckStrSuffix) && msg.Reply != "" && len(msg.Data) == 0 {
			err := msg.Respond(nil)
			if err != nil {
				logger.Log.Error().Err(err).Str("msgName", msg.Subject).Msg("deal wait success error")
			}
			return
		}
		userHandler(msg)
	}
	var un UNOptions
	for _, o := range opts {
		if err := o(&un); err != nil {
			panic(err)
		}
	}
	if un.ChanCount == 0 && len(un.Queues) == 0 {
		un.ChanCount = 1
	}

	if un.QueueChanSize == 0 {
		un.QueueChanSize = defaultQueueChanSize
	}
	isCreateQueues := false
	queues := un.Queues
	if len(queues) == 0 {
		queues = make([]chan *nats.Msg, un.ChanCount)
		for i := 0; i < un.ChanCount; i++ {
			queues[i] = make(chan *nats.Msg, un.QueueChanSize)
		}
		isCreateQueues = true
	}

	unc := &ServerUserNatsClient[T, US]{
		NatsClient:     NewNatsClient(urls, un.opts...),
		queues:         queues,
		userHandler:    uh,
		isCreateQueues: isCreateQueues,
		closeCh:        make(chan struct{}, len(queues)),
	}
	unc.startUserChan()
	return unc
}

func newServerUserNatsClientForCluster[T any, US ServerUserSubjectPtr[T]](
	c *NatsClient, queues []chan *nats.Msg,
) *ServerUserNatsClient[T, US] {
	unc := &ServerUserNatsClient[T, US]{
		NatsClient: c,
		queues:     queues,
	}

	return unc
}

type ServerIntUserSubject struct {
	ServerType string
	ServerId   int64
	RoleId     int64
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

func (u *ServerIntUserSubject) CreateSubjForCallSize() int {
	return len(u.ServerType) + utils.CountIntByte(u.ServerId) + utils.CountIntByte(u.RoleId) + 2
}

func (u *ServerIntUserSubject) CreateSubjForCall(bytes *objectpool.Bytes) {
	bytes.Reset()
	bytes.WriteString(u.ServerType)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.ServerId)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.RoleId)
}

func (u *ServerIntUserSubject) ParseSubjForCall(s string) error {
	i1 := strings.LastIndexByte(s, '/')
	if i1 == -1 {
		return define.ErrInvalidUserSubj
	}
	roleId, err := strconv.ParseInt(s[i1+1:], 10, 64)
	if err != nil {
		return errors.Join(define.ErrInvalidUserSubj, err)
	}
	u.RoleId = roleId
	return nil
}

type ServerStringUserSubject struct {
	ServerType string
	ServerId   int64
	RoleId     string
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

func (u *ServerStringUserSubject) CreateSubjForCallSize() int {
	return len(u.ServerType) + utils.CountIntByte(u.ServerId) + len(u.RoleId) + 2
}

func (u *ServerStringUserSubject) CreateSubjForCall(bytes *objectpool.Bytes) {
	bytes.Reset()
	bytes.WriteString(u.ServerType)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.ServerId)
	bytes.WriteBytes('/')
	bytes.WriteString(u.RoleId)
}

func (u *ServerStringUserSubject) ParseSubjForCall(s string) error {
	i1 := strings.LastIndexByte(s, '/')
	if i1 == -1 {
		return define.ErrInvalidUserSubj
	}

	u.RoleId = s[i1+1:]
	return nil
}

func (nc *ServerUserNatsClient[T, US]) Close() {
	nc.NatsClient.Close()
	if nc.isCreateQueues {
		for _, ch := range nc.queues {
			close(ch)
		}

		for range len(nc.queues) {
			<-nc.closeCh
		}
	}
}

func (nc *ServerUserNatsClient[T, US]) startUserChan() {
	for _, ch := range nc.queues {
		go func(ch chan *nats.Msg) {
			for msg := range ch {
				nc.userHandler(msg)
			}
			nc.closeCh <- struct{}{}
		}(ch)
	}
}

// SubscribeUser  subscribe user topic
// param us not escapes to heap
func (nc *ServerUserNatsClient[T, US]) SubscribeUser(us US) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := string(b.Bytes())
	if _, ok := nc.subs.Get(subj); ok {
		return false
	}
	index := us.ToHash() % uint64(len(nc.queues))
	sub, err := nc.conn.ChanSubscribe(subj, nc.queues[index])
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Msg("ClientSubscribeServerUser")
		return false
	}
	if !nc.subs.SetIfAbsent(subj, sub) {
		err := sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Msg("ClientSubscribeServerUser for Unsubscribe")
		}
		return false
	}
	logger.Log.Debug().Str("subj", subj).Msg("ClientSubscribeServerUser")
	return true
}

// SubscribeUserWaitSuccess SubscribeUser and wait for success
func (nc *ServerUserNatsClient[T, US]) SubscribeUserWaitSuccess(us US) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := string(b.Bytes())
	if _, ok := nc.subs.Get(subj); ok {
		return false
	}
	index := us.ToHash() % uint64(len(nc.queues))
	sub, err := nc.conn.ChanSubscribe(subj, nc.queues[index])
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
	logger.Log.Debug().Str("subj", subj).Msg("ClientSubscribeServerUser")

	// wait for success
	_, err = nc.conn.Request(subj+waitSuccessCheckStr, nil, nc.timeout)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Msg("SubscribeUserWaitSuccess error")
		return false
	}
	return true
}

// QueueSubscribeUser queue subscribe user topic
// param us not escapes to heap
func (nc *ServerUserNatsClient[T, US]) QueueSubscribeUser(us US, handler nats.MsgHandler) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := string(b.Bytes())
	if _, ok := nc.subs.Get(subj); ok {
		return false
	}
	group := subj[:len(subj)-2]
	index := us.ToHash() % uint64(len(nc.queues))
	sub, err := nc.conn.ChanQueueSubscribe(subj, group, nc.queues[index])
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

// QueueSubscribeUserWaitSuccess QueueSubscribeUser and wait for success
func (nc *ServerUserNatsClient[T, US]) QueueSubscribeUserWaitSuccess(us US) bool {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := string(b.Bytes())
	if _, ok := nc.subs.Get(subj); ok {
		return false
	}
	group := subj[:len(subj)-2]
	index := us.ToHash() % uint64(len(nc.queues))
	sub, err := nc.conn.ChanQueueSubscribe(subj, group, nc.queues[index])
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
	// wait for success
	_, err = nc.conn.Request(subj+waitSuccessCheckStr, nil, nc.timeout)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Msg("QueueSubscribeUserWaitSuccess error")
		return false
	}
	return true
}

// UnsubscribeUser  unsubscribe user topic
// param us not escapes to heap
func (nc *ServerUserNatsClient[T, US]) UnsubscribeUser(us US) {
	b := objectpool.GetBytes(0)
	defer objectpool.PutBytes(b)
	us.CreateSubj(b)
	subj := b.String()
	if v, ok := nc.subs.GetAndRemove(subj); ok {
		if v.IsValid() {
			err := v.Drain()
			if err != nil {
				logger.Log.Error().Err(err).Str("subj", subj).Msg("Client[Un]subscribeUser Drain error")
			}
		}
		logger.Log.Debug().Str("subj", subj).Msg("Client[Un]subscribeUser")
	}
}

// PublishUser publish msg to user topic
// param us,pubMsg escapes to heap
// recommended use ClientPublishServerUser
func (nc *ServerUserNatsClient[T, US]) PublishUser(c ctx.IContext, us US, pubMsg proto.Message) *berror.ErrMsg {
	traceSize := 0
	var err error
	var traceCtx ctx.Trace
	if c != nil {
		traceCtx = c.GetTrace()
		if traceCtx != nil {
			oldServerId, oldServerType := traceCtx.GetServerIdAndType()
			traceCtx.SetServerIdAndType(baseenv.GetConfig().ServerId, baseenv.GetConfig().ServerType)
			defer traceCtx.SetServerIdAndType(oldServerId, oldServerType)
			traceSize = traceCtx.TraceMarshalSize()
		}
	}
	if traceSize > math.MaxUint16 {
		return berror.NewProtocolStr("trace data too long,max size is 65535")
	}
	messageName := string(proto.MessageName(pubMsg))
	size := us.CreateSubjForCallSize() + 1 + len(messageName)
	msgSize := proto.Size(pubMsg)
	b := objectpool.GetBytes(size + 2 + traceSize + msgSize)
	defer objectpool.PutBytes(b)
	us.CreateSubjForCall(b)
	b.WriteBytes('.')
	b.WriteString(messageName)
	if traceSize > 0 {
		b.Data = append(b.Data, byte(traceSize), byte(traceSize>>8))
		b.Data, err = traceCtx.TraceMarshalAppend(b.Data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		b.Data = append(b.Data, 0, 0)
	}
	b.Data, err = proto.MarshalOptions{}.MarshalAppend(b.Data, pubMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}

	err = nc.conn.Publish(utils.BytesToString(b.Data[:size]), b.Data[size:])
	return berror.NewProtocolErr(err)
}

// RequestUser user topic rpc
// us,reqMsg,out escapes to heap
// recommended use ClientRequestServerUser
func (nc *ServerUserNatsClient[T, US]) RequestUser(c ctx.IContext, us US, reqMsg proto.Message, out proto.Message) *berror.ErrMsg {
	traceSize := 0
	var err error
	var traceCtx ctx.Trace
	if c != nil {
		traceCtx = c.GetTrace()
		if traceCtx != nil {
			oldServerId, oldServerType := traceCtx.GetServerIdAndType()
			traceCtx.SetServerIdAndType(baseenv.GetConfig().ServerId, baseenv.GetConfig().ServerType)
			defer traceCtx.SetServerIdAndType(oldServerId, oldServerType)
			traceSize = traceCtx.TraceMarshalSize()
		}
	}
	if traceSize > math.MaxUint16 {
		return berror.NewProtocolStr("trace data too long,max size is 65535")
	}
	messageName := string(proto.MessageName(reqMsg))
	size := us.CreateSubjForCallSize() + 1 + len(messageName)
	msgSize := proto.Size(reqMsg)
	b := objectpool.GetBytes(size + 2 + traceSize + msgSize)
	defer objectpool.PutBytes(b)
	us.CreateSubjForCall(b)
	b.WriteBytes('.')
	b.WriteString(messageName)
	if traceSize > 0 {
		b.Data = append(b.Data, byte(traceSize), byte(traceSize>>8))
		b.Data, err = traceCtx.TraceMarshalAppend(b.Data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		b.Data = append(b.Data, 0, 0)
	}
	b.Data, err = proto.MarshalOptions{}.MarshalAppend(b.Data, reqMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	outMsg, err := nc.conn.Request(utils.BytesToString(b.Data[:size]), b.Data[size:], nc.timeout)
	if err != nil {
		return berror.NewProtocolStr(utils.BytesToString(b.Data[:size]) + "[" + nc.conn.ConnectedAddr() + "]:" + err.Error())
	}

	return NatsUnmarshalResponseWithout(outMsg.Data, out)
}

// ClientPublishServerUser publish msg to user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see NatsClient.PublishServerUser
// create use NewSUClientPublishUser
type ClientPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]] struct {
	Pub T1
	Us  T
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClientPublishServerUser[T, T1, US, PUB]) Reset() {
	*r = ClientPublishServerUser[T, T1, US, PUB]{}
}

// NewSUClientPublishUser create ClientPublishServerUser for objectpool
func NewSUClientPublishUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]]() *ClientPublishServerUser[T, T1, US, PUB] {
	c := objectpool.Get[ClientPublishServerUser[T, T1, US, PUB]]()
	return c
}

// Publish more info see NatsClient.PublishServerUser
func (r *ClientPublishServerUser[T, T1, US, PUB]) Publish(nc *ServerUserNatsClient[T, US], c ctx.IContext) *berror.ErrMsg {
	err := nc.PublishUser(c, (US)(&r.Us), (PUB)(&r.Pub))
	return err
}

func (r *ClientPublishServerUser[T, T1, US, PUB]) Free() {
	objectpool.Put(r)
}

// ClientRequestServerUser rpc user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see NatsClient.RequestUser
// create use NewClientRequestServerUser
type ClientRequestServerUser[T, T1, T2 any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]] struct {
	Req  T1
	Resp T2
	Us   T
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClientRequestServerUser[T, T1, T2, US, REQ, RESP]) Reset() {
	*r = ClientRequestServerUser[T, T1, T2, US, REQ, RESP]{}
}

// NewClientRequestServerUser create ClientRequestServerUser for objectpool
func NewClientRequestServerUser[T, T1, T2 any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]]() *ClientRequestServerUser[T, T1, T2, US, REQ, RESP] {
	c := objectpool.Get[ClientRequestServerUser[T, T1, T2, US, REQ, RESP]]()
	return c
}

// Request more info see NatsClient.Request
func (r *ClientRequestServerUser[T, T1, T2, US, REQ, RESP]) Request(nc *ServerUserNatsClient[T, US], c ctx.IContext) *berror.ErrMsg {
	err := nc.RequestUser(c, (US)(&r.Us), (REQ)(&r.Req), (RESP)(&r.Resp))
	return err
}

func (r *ClientRequestServerUser[T, T1, T2, US, REQ, RESP]) Free() {
	objectpool.Put(r)
}
