package natsclient

import (
	"hash/crc32"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/cmap"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/safego"
	"github.com/ravinggo/game/common/utils"
)

const (
	totalSizeLen    = 4
	maxProtoMsgSize = math.MaxInt32 >> 8
)

type NatsClient struct {
	subs     cmap.ConcurrentMap[string, *nats.Subscription]
	name     string
	urls     string
	conn     *nats.Conn
	serverId int64
	closed   int32
	f        nats.MsgHandler
	timeout  time.Duration
}

func NewNatsClient(name string, urls string, timeout time.Duration) *NatsClient {
	if timeout <= 0 {
		timeout = time.Second * 10
	}
	nc := &NatsClient{
		subs:    cmap.New[*nats.Subscription](),
		name:    name,
		urls:    urls,
		timeout: timeout,
	}
	c, err := nats.Connect(
		urls, nats.ReconnectWait(time.Millisecond*10), nats.MaxReconnects(math.MaxInt64),
		nats.PingInterval(time.Second*3), nats.MaxPingsOutstanding(2), nats.Timeout(time.Second),
		nats.DrainTimeout(time.Second*5), nats.Name(name),
		nats.DisconnectErrHandler(
			func(conn *nats.Conn, err error) {
				if atomic.LoadInt32(&nc.closed) == 0 {
					logger.Log.Error().Err(err).Str("urls", urls).Str("nats-server", conn.ConnectedAddr()).Msg("nats disconnected")
				}
			},
		),
		nats.ReconnectHandler(
			func(conn *nats.Conn) {
				logger.Log.Warn().Str("nats-server", conn.ConnectedAddr()).Msg("nats reconnected")
			},
		),
		nats.ClosedHandler(
			func(conn *nats.Conn) {
				logger.Log.Warn().Str("nats-server", conn.ConnectedAddr()).Msg("nats closed")
			},
		),
	)
	if err != nil {
		panic(err)
	}
	nc.conn = c
	return nc
}

// Close 关闭nats
func (this_ *NatsClient) Close() {
	this_.subs.IterCb(
		func(key string, v *nats.Subscription) bool {
			if v.IsValid() {
				err := v.Drain()
				if err != nil {
					logger.Log.Warn().Str("subj", key).Err(err).Msg("Drain error")
				}
			}
			return true
		},
	)
	this_.subs.IterCb(
		func(key string, v *nats.Subscription) bool {
			for v.IsValid() {
				time.Sleep(time.Millisecond * 10)
			}
			return true
		},
	)
}

// Shutdown 关闭NATS
func (this_ *NatsClient) Shutdown() {
	if atomic.CompareAndSwapInt32(&this_.closed, 0, 1) {
		_ = this_.conn.FlushTimeout(time.Second * 3)
		this_.conn.Close()
	}
}

// Subscribe 订阅主题
func (this_ *NatsClient) Subscribe(subj string, h nats.MsgHandler) {
	if _, ok := this_.subs.Get(subj); ok {
		logger.Log.Panic().Str("subj", subj).Msg("subj had Subscribed")
	}
	logger.Log.Info().Str("urls", this_.urls).Str("subj", subj).Msg("Subscribe")
	sub, err := this_.conn.Subscribe(subj, h)
	if err != nil {
		logger.Log.Panic().Err(err).Str("subj", subj).Msg("Subscribe error")
	}
	this_.subs.Set(subj, sub)
}

// QueueSubscribe 订阅Topic for queue
func (this_ *NatsClient) QueueSubscribe(subj string, h nats.MsgHandler) {
	if _, ok := this_.subs.Get(subj); ok {
		logger.Log.Panic().Str("subj", subj).Msg("subj had Subscribed")
	}
	group := strings.ReplaceAll(subj, ">", "group")
	logger.Log.Info().Str("urls", this_.urls).Str("subj", subj).Str("group", group).Msg("SubscribeHandler")

	cb := this_.f
	if h != nil {
		cb = h
	}
	sub, err := this_.conn.QueueSubscribe(subj, group, cb)
	if err != nil {
		logger.Log.Panic().Err(err).Str("subj", subj).Msg("Subscribe error")
	}
	this_.subs.Set(subj, sub)
}

// Unsubscribe 解除订阅
func (this_ *NatsClient) Unsubscribe(subj string) {
	if s, ok := this_.subs.Get(subj); ok {
		logger.Log.Info().Str("subj", subj).Msg("Unsubscribe")
		_ = s.Unsubscribe()
		this_.subs.Remove(subj)
	}
}

type ClientPublish[T any, PUB define.ProtoMessagePtr[T]] struct {
	Pub T
}

func NewClientPublish[T any, PUB define.ProtoMessagePtr[T]]() *ClientPublish[T, PUB] {
	return objectpool.Get[ClientPublish[T, PUB]]()
}

func (r *ClientPublish[T, PUB]) Reset() {
	proto.Reset((PUB)(&r.Pub))
}

func (r *ClientPublish[T, PUB]) Publish(nc *NatsClient, c ctx.IContext) *berror.ErrMsg {
	err := nc.Publish(c, (PUB)(&r.Pub))
	return err
}

func (r *ClientPublish[T, PUB]) PublishToServer(nc *NatsClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	err := nc.PublishToServer(c, toServerId, (PUB)(&r.Pub))
	return err
}

// Publish  msg to any one instance of the server
func (this_ *NatsClient) Publish(c ctx.IContext, msg proto.Message) *berror.ErrMsg {
	return this_.PublishToServer(c, 0, msg)
}

// PublishToServer publish msg to specified server instance of toServerId
func (this_ *NatsClient) PublishToServer(c ctx.IContext, toServerId int64, pubMsg proto.Message) *berror.ErrMsg {
	msgName := string(proto.MessageName(pubMsg))
	msgNameSize := 21 + len(msgName)
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
	size := 2 + proto.Size(pubMsg) + traceSize
	if toServerId > 0 {
		size += msgNameSize
	}
	b := objectpool.GetSlice[byte](size)
	defer objectpool.PutSlice(b)

	data := b.Data
	if toServerId > 0 {
		data = b.Data[msgNameSize : cap(b.Data)-msgNameSize][:0]
		index := strings.LastIndexByte(msgName, '.')
		b.Data = append(b.Data, msgName[:index]...) // index != -1
		b.Data = append(b.Data, '.')
		b.Data = strconv.AppendInt(b.Data, toServerId, 10)
		b.Data = append(b.Data, msgName[index:]...)
		msgName = utils.BytesToString(b.Data)
	}

	if traceSize > 0 {
		if traceSize > math.MaxUint16 {
			return berror.NewProtocolStr("trace data too long,max size is 65535")
		}
		data = append(data, byte(traceSize), byte(traceSize>>8))
		data, err = traceCtx.TraceMarshalAppend(data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		data = append(data, 0, 0)
	}
	data, err = proto.MarshalOptions{}.MarshalAppend(data, pubMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}

	err = this_.conn.Publish(msgName, data)
	return berror.NewProtocolErr(err)
}

func (this_ *NatsClient) PublishRawData(c ctx.IContext, toServerId int64, msgName string, msgData []byte) *berror.ErrMsg {
	msgNameSize := 21 + len(msgName)
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
	size := 2 + len(msgData) + traceSize
	if toServerId > 0 {
		size += msgNameSize
	}
	b := objectpool.GetSlice[byte](size)
	defer objectpool.PutSlice(b)

	data := b.Data
	if toServerId > 0 {
		data = b.Data[msgNameSize:]
		index := strings.LastIndexByte(msgName, '.')
		b.Data = append(b.Data, msgName[:index]...) // index != -1
		b.Data = append(b.Data, '.')
		b.Data = strconv.AppendInt(b.Data, toServerId, 10)
		b.Data = append(b.Data, msgName[index:]...)
		msgName = utils.BytesToString(b.Data)
	}

	if traceSize > 0 {
		if traceSize > math.MaxUint16 {
			return berror.NewProtocolStr("trace data too long,max size is 65535")
		}
		data = append(data, byte(traceSize), byte(traceSize>>8))
		data, err = traceCtx.TraceMarshalAppend(data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		data = append(data, 0, 0)
	}
	if len(msgData) > 0 {
		data = append(data, msgData...)
	}
	err = this_.conn.Publish(msgName, data)
	return berror.NewProtocolErr(err)
}

func (this_ *NatsClient) Request(c ctx.IContext, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	return this_.RequestToServer(c, 0, reqMsg, respMsg)
}

type Request[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]] struct {
	Req  T
	Resp T1
}

func (r *Request[T, T1, REQ, RESP]) Reset() {
	proto.Reset((REQ)(&r.Req))
	proto.Reset((RESP)(&r.Resp))
}

func (r *Request[T, T1, REQ, RESP]) Request(nc *NatsClient, c ctx.IContext) *berror.ErrMsg {
	err := nc.Request(c, (REQ)(&r.Req), (RESP)(&r.Resp))
	return err
}

func (r *Request[T, T1, REQ, RESP]) RequestToServer(nc *NatsClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	err := nc.RequestToServer(c, toServerId, (REQ)(&r.Req), (RESP)(&r.Resp))
	return err
}

// RequestToServer send rpc to specified server instance of toServerId
// req,resp : Will definitely escape to the heap because proto.MessageName and proto.Marshal and proto.Unmarshal
func (this_ *NatsClient) RequestToServer(c ctx.IContext, toServerId int64, req proto.Message, resp proto.Message) *berror.ErrMsg {
	msgName := string(proto.MessageName(req))
	msgNameSize := 21 + len(msgName)
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
	size := 2 + proto.Size(req) + traceSize
	if toServerId > 0 {
		size += msgNameSize
	}
	b := objectpool.GetSlice[byte](size)
	defer objectpool.PutSlice(b)

	data := b.Data
	if toServerId > 0 {
		data = b.Data[msgNameSize : cap(b.Data)-msgNameSize][:0]
		index := strings.LastIndexByte(msgName, '.')
		b.Data = append(b.Data, msgName[:index]...) // index != -1
		b.Data = append(b.Data, '.')
		b.Data = strconv.AppendInt(b.Data, toServerId, 10)
		b.Data = append(b.Data, msgName[index:]...)
		msgName = utils.BytesToString(b.Data)
	}

	if traceSize > 0 {
		if traceSize > math.MaxUint16 {
			return berror.NewProtocolStr("trace data too long,max size is 65535")
		}
		data = append(data, byte(traceSize), byte(traceSize>>8))
		data, err = traceCtx.TraceMarshalAppend(data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	} else {
		data = append(data, 0, 0)
	}
	data, err = proto.MarshalOptions{}.MarshalAppend(data, req)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	var natsMsg *nats.Msg
	natsMsg, err = this_.conn.Request(msgName, data, this_.timeout)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	return NatsUnmarshalResponseWithout(natsMsg.Data, resp)
}

func natsUnmarshalResponse(data []byte) (string, []byte, *berror.ErrMsg) {
	if len(data) < totalSizeLen {
		return "", nil, berror.NewProtocolStr("invalid message")
	}
	msgNameLen := int(data[0])
	if len(data) < totalSizeLen+msgNameLen {
		return "", nil, berror.NewProtocolStr("invalid msg data")
	}
	dataLen := int(data[1]) | (int(data[2]) << 8) | (int(data[3]) << 16)
	dataIndex := totalSizeLen + msgNameLen
	if len(data) != dataIndex+dataLen {
		return "", nil, berror.NewProtocolStr("invalid msg data")
	}
	msgName := utils.BytesToString(data[totalSizeLen:dataIndex])
	return msgName, data[dataIndex:], nil
}

func NatsUnmarshalResponseWithout(d []byte, respMsg proto.Message) *berror.ErrMsg {
	msgName, data, err := natsUnmarshalResponse(d)
	if err != nil {
		return err
	}
	if msgName == berror.ErrMsgName {
		errMsg := &basepb.ErrorMessage{}
		e := proto.Unmarshal(data, errMsg)
		if e != nil {
			return berror.NewProtocolErr(e)
		}
		return (*berror.ErrMsg)(errMsg)
	}
	outMsgName := string(proto.MessageName(respMsg))
	if outMsgName != msgName {
		return berror.NewProtocolStr("response msg is " + msgName + ", not is " + outMsgName)
	}
	e := proto.Unmarshal(data, respMsg)
	if e != nil {
		return berror.NewProtocolErr(e)
	}
	return nil
}

func NatsMsgReplyError(reply *nats.Msg, err *berror.ErrMsg) *berror.ErrMsg {
	if err == nil {
		return nil
	}
	return natsMsgReplyOne(reply, (*basepb.ErrorMessage)(err))
}

func NatsMsgReply(reply *nats.Msg, respMsgS ...proto.Message) *berror.ErrMsg {
	if len(respMsgS) == 0 {
		return nil
	}
	if len(respMsgS) == 1 {
		return natsMsgReplyOne(reply, respMsgS[0])
	}

	totalSize := 0
	for _, m := range respMsgS {
		s, err := natsMarshalResponseSize(m)
		if err != nil {
			return err
		}
		totalSize += s
	}
	b := objectpool.GetBytes(totalSize)
	defer objectpool.PutBytes(b)

	for _, m := range respMsgS {
		err := natsMarshalAppendResponse(b, m)
		if err != nil {
			return err
		}
	}
	return berror.NewProtocolErr(reply.Respond(b.Data))
}

func natsMsgReplyOne(reply *nats.Msg, respMsg proto.Message) *berror.ErrMsg {
	msgName := string(proto.MessageName(respMsg))
	msgNameSize := len(msgName)
	if msgNameSize > math.MaxUint8 {
		return berror.NewProtocolStr("respMsg name too long")
	}
	msgSize := proto.Size(respMsg)
	if msgSize > maxProtoMsgSize {
		return berror.NewProtocolStr("respMsg data too long")
	}
	b := objectpool.GetBytes(totalSizeLen + msgNameSize + msgSize)
	defer objectpool.PutBytes(b)
	b.WriteBytes(byte(msgNameSize))
	b.WriteBytes(byte(msgSize), byte(msgSize>>8), byte(msgSize>>16))
	b.WriteString(msgName)
	var err error
	b.Data, err = proto.MarshalOptions{}.MarshalAppend(b.Data, respMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	return berror.NewProtocolErr(reply.Respond(b.Data))
}

func natsMarshalResponseSize(respMsg proto.Message) (int, *berror.ErrMsg) {
	msgSize := proto.Size(respMsg)
	if msgSize > maxProtoMsgSize {
		return 0, berror.NewProtocolStr("respMsg data too long")
	}
	return totalSizeLen + len(proto.MessageName(respMsg)) + msgSize, nil
}

func natsMarshalAppendResponse(b *objectpool.Bytes, respMsg proto.Message) *berror.ErrMsg {
	msgName := string(proto.MessageName(respMsg))
	msgNameLen := len(msgName)
	if msgNameLen > math.MaxUint8 {
		return berror.NewProtocolStr("respMsg name too long")
	}
	b.WriteBytes(byte(msgNameLen))
	msgSize := proto.Size(respMsg)
	b.WriteBytes(byte(msgSize), byte(msgSize>>8), byte(msgSize>>16))
	b.WriteString(msgName)
	var err error
	b.Data, err = proto.MarshalOptions{}.MarshalAppend(b.Data, respMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	return nil
}

func (this_ *NatsClient) RequestRaw(c ctx.IContext, toServerId int64, reqMsgName string, reqMsgData []byte) ([]byte, *berror.ErrMsg) {
	msgNameSize := 21 + len(reqMsgName)
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

	size := 2 + len(reqMsgData) + traceSize
	if toServerId > 0 {
		size += msgNameSize
	}
	b := objectpool.GetSlice[byte](size)
	defer objectpool.PutSlice(b)

	data := b.Data
	if toServerId > 0 {
		data = b.Data[msgNameSize:]
		index := strings.LastIndexByte(reqMsgName, '.')
		b.Data = append(b.Data, reqMsgName[:index]...) // index != -1
		b.Data = append(b.Data, '.')
		b.Data = strconv.AppendInt(b.Data, toServerId, 10)
		b.Data = append(b.Data, reqMsgName[index:]...)
		reqMsgName = utils.BytesToString(b.Data)
	}

	if traceSize > 0 {
		if traceSize > math.MaxUint16 {
			return nil, berror.NewProtocolStr("trace data too long,max size is 65535")
		}
		data = append(data, byte(traceSize), byte(traceSize>>8))
		data, err = traceCtx.TraceMarshalAppend(data)
		if err != nil {
			return nil, berror.NewProtocolErr(err)
		}
	}
	if len(reqMsgData) > 0 {
		data = append(data, reqMsgData...)
	}
	natsMsg, err := this_.conn.Request(reqMsgName, data, this_.timeout)
	if err != nil {
		return nil, berror.NewProtocolErr(err)
	}
	return natsMsg.Data, nil
}

type UserSubject interface {
	ToHash() int64
	CreateSubj() string
	CreatePublishSize() int
	CreatePublish(bytes *objectpool.Bytes)
}

type IntUserSubject struct {
	ServerType string
	ServerId   int64
	RoleId     int64
	MsgName    string
}

func (u *IntUserSubject) CreateSubj() string {
	b := utils.NewStringBuilder(256)
	b.WriteString(u.ServerType)
	b.WriteByte('/')
	b.WriteInt(u.ServerId)
	b.WriteByte('/')
	b.WriteInt(u.RoleId)
	b.WriteString(".>")
	return b.String()
}
func (u *IntUserSubject) ToHash() int64 {
	return u.RoleId
}

func (u *IntUserSubject) CreatePublishSize() int {
	return len(u.ServerType) + utils.CountIntByte(u.ServerId) + utils.CountIntByte(u.RoleId) + len(u.MsgName) + 3
}

func (u *IntUserSubject) CreatePublish(bytes *objectpool.Bytes) {
	bytes.Reset()
	bytes.WriteString(u.ServerType)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.ServerId)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.RoleId)
	bytes.WriteBytes('.')
	bytes.WriteString(u.MsgName)
}

type StringUserSubject struct {
	ServerType string
	ServerId   int64
	RoleId     string
	MsgName    string
}

func (u *StringUserSubject) CreateSubj() string {
	b := utils.NewStringBuilder(256)
	b.WriteString(u.ServerType)
	b.WriteByte('/')
	b.WriteInt(u.ServerId)
	b.WriteByte('/')
	b.WriteString(u.RoleId)
	b.WriteString(".>")
	return b.String()
}

func (u *StringUserSubject) ToHash() int64 {
	return int64(crc32.ChecksumIEEE(utils.StringToBytes(u.RoleId)))
}

func (u *StringUserSubject) CreatePublishSize() int {
	return len(u.ServerType) + utils.CountIntByte(u.ServerId) + len(u.RoleId) + len(u.MsgName) + 3
}

func (u *StringUserSubject) CreatePublish(bytes *objectpool.Bytes) {
	bytes.Reset()
	bytes.WriteString(u.ServerType)
	bytes.WriteBytes('/')
	bytes.WriteInt(u.ServerId)
	bytes.WriteBytes('/')
	bytes.WriteString(u.RoleId)
	bytes.WriteBytes('.')
	bytes.WriteString(u.MsgName)
}

// SubscribeUser Subscribe user topic
func (this_ *NatsClient) SubscribeUser(us UserSubject, queue chan *nats.Msg) {
	subj := us.CreateSubj()
	if _, ok := this_.subs.Get(subj); ok {
		return
	}
	logger.Log.Info().Str("subj", subj).Msg("QueueSubscribeUser")
	sub, err := this_.conn.ChanSubscribe(subj, queue)
	if err != nil {
		logger.Log.Error().Err(err).Str("subj", subj).Msg("QueueSubscribeUser")
	}
	this_.subs.Set(subj, sub)
}

func (this_ *NatsClient) UnsubscribeUser(us UserSubject) {
	subj := us.CreateSubj()
	if v, ok := this_.subs.Get(subj); ok {
		this_.subs.Remove(subj)
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

func (this_ *NatsClient) PublishUser(c ctx.IContext, us UserSubject, pubMsg proto.Message) *berror.ErrMsg {
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
	size := us.CreatePublishSize()
	msgSize := proto.Size(pubMsg)
	b := objectpool.GetBytes(size + 2 + traceSize + msgSize)
	defer objectpool.PutBytes(b)
	us.CreatePublish(b)
	if traceSize > 0 {
		b.Data = append(b.Data, byte(traceSize), byte(traceSize>>8))
		b.Data, err = traceCtx.TraceMarshalAppend(b.Data)
		if err != nil {
			return berror.NewProtocolErr(err)
		}
	}
	_, err = proto.MarshalOptions{}.MarshalAppend(b.Data, pubMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}

	err = this_.conn.Publish(utils.BytesToString(b.Data[:size]), b.Data[size:])
	return berror.NewProtocolErr(err)
}

func (this_ *NatsClient) RequestUser(c ctx.IContext, us UserSubject, reqMsg proto.Message, out proto.Message) *berror.ErrMsg {
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

	size := us.CreatePublishSize()
	msgSize := proto.Size(reqMsg)
	b := objectpool.GetBytes(size + 2 + traceSize + msgSize)
	defer objectpool.PutBytes(b)
	us.CreatePublish(b)
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

type RequestUser[T any] struct {
	Ret T
	US  UserSubject
}

func (r *RequestUser[T]) Request(nc *NatsClient, c ctx.IContext, reqMsg proto.Message) *berror.ErrMsg {
	var a any = &r.Ret
	err := nc.RequestUser(c, r.US, reqMsg, a.(proto.Message))
	return err
}
