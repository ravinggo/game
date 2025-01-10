package natsclient

import (
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

// NewNatsClient one nats cluster client
// encapsulates the calling design of the entire framework
// param name Usually the server name ( baseenv.Config.ServerType )
// param urls a nats cluster urls
// param timeout for request
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
		nats.ErrorHandler(
			func(conn *nats.Conn, subscription *nats.Subscription, err error) {
				logger.Log.Warn().Str("nats-server", conn.ConnectedAddr()).Err(err).Msg("nats error")
			},
		),
	)
	if err != nil {
		panic(err)
	}
	nc.conn = c
	return nc
}

// Close just Drain all subscriptions
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

// Shutdown flush all subscriptions and close connection
func (this_ *NatsClient) Shutdown() {
	if atomic.CompareAndSwapInt32(&this_.closed, 0, 1) {
		_ = this_.conn.FlushTimeout(time.Second * 3)
		this_.conn.Close()
	}
}

// Subscribe topic
func (this_ *NatsClient) Subscribe(subj string, h nats.MsgHandler) bool {
	if _, ok := this_.subs.Get(subj); ok {
		logger.Log.Error().Str("subj", subj).Msg("subj had Subscribed")
		return false
	}
	sub, err := this_.conn.Subscribe(subj, h)
	if err != nil {
		logger.Log.Panic().Err(err).Str("subj", subj).Msg("Subscribe error")
		return false
	}

	if !this_.subs.SetIfAbsent(subj, sub) {
		err = sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Msg("Subscribe for Unsubscribe")
		}
		return false
	}
	logger.Log.Info().Str("urls", this_.urls).Str("subj", subj).Msg("Subscribe")
	return true
}

// QueueSubscribe queue subscribe
func (this_ *NatsClient) QueueSubscribe(subj string, h nats.MsgHandler) bool {
	if _, ok := this_.subs.Get(subj); ok {
		logger.Log.Error().Str("subj", subj).Msg("subj had Subscribed")
		return false
	}
	group := strings.ReplaceAll(subj, ">", "group")
	sub, err := this_.conn.QueueSubscribe(subj, group, h)
	if err != nil {
		logger.Log.Panic().Err(err).Str("subj", subj).Msg("Subscribe error")
		return false
	}
	if !this_.subs.SetIfAbsent(subj, sub) {
		err = sub.Unsubscribe()
		if err != nil {
			logger.Log.Error().Err(err).Str("subj", subj).Msg("QueueSubscribe for Unsubscribe")
		}
		return false
	}
	logger.Log.Info().Str("urls", this_.urls).Str("subj", subj).Msg("QueueSubscribe")
	return true
}

// Unsubscribe topic
func (this_ *NatsClient) Unsubscribe(subj string) {
	if v, ok := this_.subs.GetAndRemove(subj); ok {
		logger.Log.Info().Str("subj", subj).Msg("Unsubscribe")
		go func() {
			defer safego.Recover()
			if v.IsValid() {
				err := v.Drain()
				if err != nil {
					logger.Log.Error().Err(err).Str("subj", subj).Msg("Client[Un]subscribeUser Drain error")
				}
			}
		}()
	}
}

// ClientPublish Generic Implementation : publish for topic
// designed to use objectpool, because Pub will always escape to heap
// for more information, see NatsClient.Publish and NatsClient.PublishToServer
// create use NewClientPublish
type ClientPublish[T any, PUB define.ProtoMessagePtr[T]] struct {
	Pub    T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// NewClientPublish create a ClientPublish from objectpool
func NewClientPublish[T any, PUB define.ProtoMessagePtr[T]]() *ClientPublish[T, PUB] {
	c := objectpool.Get[ClientPublish[T, PUB]]()
	c.forNew = 1
	return c
}

// Reset implement define.Clear
func (r *ClientPublish[T, PUB]) Reset() {
	*r = ClientPublish[T, PUB]{}
}

// Publish msg to serverId is 0 or a cluster server node
// more information, see NatsClient.Publish and NatsClient.PublishToServer
func (r *ClientPublish[T, PUB]) Publish(nc *NatsClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("ClientPublish is not created by NewClientPublish")
	}
	if atomic.LoadInt32(&nc.closed) != 0 {
		err := nc.Publish(c, (PUB)(&r.Pub))
		objectpool.Put(r)
		return err
	}
	panic("ClientPublish used")
}

// PublishToServer publish msg to specified server instance of toServerId
// more information, see NatsClient.Publish and NatsClient.PublishToServer
func (r *ClientPublish[T, PUB]) PublishToServer(nc *NatsClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("ClientPublish is not created by NewClientPublish")
	}
	if atomic.LoadInt32(&nc.closed) != 0 {
		err := nc.PublishToServer(c, toServerId, (PUB)(&r.Pub))
		objectpool.Put(r)
		return err
	}
	panic("ClientPublish used")
}

// Publish msg to serverId is 0 or a cluster server node
// param pubMsg escapes to heap
// recommended use ClientPublish.Publish
func (this_ *NatsClient) Publish(c ctx.IContext, pubMsg proto.Message) *berror.ErrMsg {
	return this_.PublishToServer(c, 0, pubMsg)
}

// PublishToServer publish msg to specified server instance of toServerId
// if toServerId == 0, PublishToServer == Publish
// // recommended use ClientPublish.PublishToServer
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

// PublishRawData publish raw data to specified server instance of toServerId
// msgName is proto message name
// msgData is proto message data
// if toServerId == 0, PublishToServer == Publish
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

// Request call rpc with serverId is 0 or a cluster server node
// respMsg is response message
// param reqMsg,respMsg escapes to heap
// recommended use ClientRequest.Request
func (this_ *NatsClient) Request(c ctx.IContext, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	return this_.RequestToServer(c, 0, reqMsg, respMsg)
}

// ClientRequest Generic Implementation : rpc for topic
// designed to use objectpool, because Req, Resp will always escape to heap
// for more information, see NatsClient.Request
// create use NewClientRequest
type ClientRequest[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]] struct {
	Req    T
	Resp   T1
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// NewClientRequest create ClientRequest for objectpool
func NewClientRequest[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]]() *ClientRequest[T, T1, REQ, RESP] {
	c := objectpool.Get[ClientRequest[T, T1, REQ, RESP]]()
	c.forNew = 1
	return c
}

// Reset implement define.Clear
func (r *ClientRequest[T, T1, REQ, RESP]) Reset() {
	*r = ClientRequest[T, T1, REQ, RESP]{}
}

// Request more info see NatsClient.Request
func (r *ClientRequest[T, T1, REQ, RESP]) Request(nc *NatsClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClientRequest please use NewClientRequest")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		err := nc.Request(c, (REQ)(&r.Req), (RESP)(&r.Resp))
		objectpool.Put(r)
		return err
	}
	panic("ClientRequest used")
}

// RequestToServer more info see NatsClient.RequestToServer
func (r *ClientRequest[T, T1, REQ, RESP]) RequestToServer(nc *NatsClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClientRequest please use NewClientRequest")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		err := nc.RequestToServer(c, toServerId, (REQ)(&r.Req), (RESP)(&r.Resp))
		objectpool.Put(r)
		return err
	}
	panic("ClientRequest used")
}

// RequestToServer send rpc to specified server instance of toServerId
// if toServerId == 0, RequestToServer == Request
// req,resp : Will definitely escape to the heap because proto.MessageName and proto.Marshal and proto.Unmarshal
func (this_ *NatsClient) RequestToServer(c ctx.IContext, toServerId int64, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	msgName := string(proto.MessageName(reqMsg))
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
	size := 2 + proto.Size(reqMsg) + traceSize
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
	data, err = proto.MarshalOptions{}.MarshalAppend(data, reqMsg)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	var natsMsg *nats.Msg
	natsMsg, err = this_.conn.Request(msgName, data, this_.timeout)
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	return NatsUnmarshalResponseWithout(natsMsg.Data, respMsg)
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

// NatsUnmarshalResponseWithout Unmarshal response data from nats.Msg.Data
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

// NatsMsgReplyError Reply to nats.Msg for an *berror.ErrMsg
func NatsMsgReplyError(reply *nats.Msg, err *berror.ErrMsg) *berror.ErrMsg {
	if err == nil {
		return nil
	}
	return natsMsgReplyOne(reply, (*basepb.ErrorMessage)(err))
}

// NatsMsgReply Reply to nats.Msg for respMsgS
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

// RequestRaw send rpc to server
// if toServerId == 0, Receiver serverId is 0 or a cluster server node
// param reqMsgName is proto message name.
// param reqMsgData is proto message data.
// return nats.Msg.Data and berror.ErrMsg.
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
