package natsclient

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/cmap"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/utils"
)

var (
	ErrInvalidRequestLength = errors.New("invalid request length")
)

type NatsClient struct {
	subs     cmap.ConcurrentMap[string, *nats.Subscription]
	name     string
	urls     string
	conn     *nats.Conn
	serverId int64
	closed   int32
	f        func(*nats.Msg)
}

func NewNatsClient(name string, urls string) *NatsClient {
	nc := &NatsClient{
		subs: cmap.New[*nats.Subscription](),
		name: name,
		urls: urls,
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

// Publish 推送数据
func (this_ *NatsClient) Publish(c ctx.IHashContext, toServerId int64, msg proto.Message) error {
	msgName := string(proto.MessageName(msg))
	msgNameSize := 21 + len(msgName)
	traceSize := 0
	var err error
	marshalCtx, ok := c.(ctx.MarshalCtx)
	if ok {
		traceSize = marshalCtx.TraceMarshalSize()
	}
	size := 2 + proto.Size(msg) + traceSize
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
		data = append(data, byte(traceSize), byte(traceSize>>8))
		data, err = marshalCtx.TraceMarshalAppend(data)
		if err != nil {
			return err
		}
	}
	data, err = proto.MarshalOptions{}.MarshalAppend(data, msg)
	if err != nil {
		return err
	}

	err = this_.conn.Publish(msgName, data)
	return err
}

// Publish 推送数据
// func (this_ *NatsClient) Publish(serverId int64, h *models.ServerHeader, msg proto.Message) *errmsg.ErrMsg {
// 	if h != nil && (h.FromServerId != this_.serverId || h.FromServerType != this_.serverType) {
// 		os, ot := h.FromServerId, h.FromServerType
// 		h.FromServerId, h.FromServerType = this_.serverId, this_.serverType
// 		defer func() {
// 			h.FromServerId, h.FromServerType = os, ot
// 		}()
// 	}
// 	n := protocol.NatsMarshalSize(h, msg)
// 	d := bytespool.GetSample(n)
// 	defer bytespool.PutSample(d)
// 	d.Data = d.Data[:0]
// 	err := protocol.NatsMarshalTo(&d.Data, h, msg)
// 	if err != nil {
// 		return err
// 	}
// 	msgName := string(proto.MessageName(msg))
// 	if serverId != 0 {
// 		n := strings.IndexByte(msgName, '.')
// 		if n == -1 {
// 			return errmsg.NewProtocolErrorInfo(
// 				fmt.Sprintf(
// 					"header.Data.TypeUrl is not a proto.MessageName:%s", msgName,
// 				),
// 			)
// 		}
//
// 		b := bytespool.GetSample(len(msgName) + 10)
// 		defer bytespool.PutSample(b)
//
// 		b.Data = b.Data[:0]
// 		b.Data = append(b.Data, msgName[:n]...)
// 		b.Data = append(b.Data, '.')
// 		b.Data = strconv.AppendInt(b.Data, serverId, 10)
// 		b.Data = append(b.Data, msgName[n:]...)
// 		msgName = *(*string)(unsafe.Pointer(&b.Data))
// 	}
// 	// 	this_.log.Info("publish", zap.String("msgName", msgName), zap.Any("msg", msg))
// 	return errmsg.NewProtocolError(this_.conn.Publish(msgName, d.Data))
// }
//
// // PublishRawData 推送数据
// func (this_ *NatsClient) PublishRawData(
// 	serverId int64,
// 	h *models.ServerHeader,
// 	msgName string,
// 	msgData []byte,
// ) *errmsg.ErrMsg {
// 	if h != nil && (h.FromServerId != this_.serverId || h.FromServerType != this_.serverType) {
// 		os, ot := h.FromServerId, h.FromServerType
// 		h.FromServerId, h.FromServerType = this_.serverId, this_.serverType
// 		defer func() {
// 			h.FromServerId, h.FromServerType = os, ot
// 		}()
// 	}
// 	n := protocol.NatsMarshalDataSize(h, msgData)
// 	d := bytespool.GetSample(n)
// 	defer bytespool.PutSample(d)
// 	d.Data = d.Data[:0]
// 	err := protocol.NatsMarshalDataTo(&d.Data, h, msgData)
// 	if err != nil {
// 		return err
// 	}
// 	if serverId != 0 {
// 		n := strings.IndexByte(msgName, '.')
// 		if n == -1 {
// 			return errmsg.NewProtocolErrorInfo(
// 				fmt.Sprintf(
// 					"header.Data.TypeUrl is not a proto.MessageName:%s", msgName,
// 				),
// 			)
// 		}
//
// 		b := bytespool.GetSample(len(msgName) + 10)
// 		defer bytespool.PutSample(b)
// 		b.Data = b.Data[:0]
// 		b.Data = append(b.Data, msgName[:n]...)
// 		b.Data = append(b.Data, '.')
// 		b.Data = strconv.AppendInt(b.Data, serverId, 10)
// 		b.Data = append(b.Data, msgName[n:]...)
// 		msgName = *(*string)(unsafe.Pointer(&b.Data))
// 	}
// 	// 	this_.log.Info("publish", zap.String("msgName", msgName), zap.Any("msg", msg))
// 	return errmsg.NewProtocolError(this_.conn.Publish(msgName, d.Data))
// }
//
// func (this_ *NatsClient) RequestWithCtx(c *ctx.Context, serverId int64, req, out proto.Message) *errmsg.ErrMsg {
// 	var h *models.ServerHeader
// 	if c != nil {
// 		h = &c.ServerHeader
// 	}
// 	return this_.Request(h, serverId, req, out)
// }
//
// func (this_ *NatsClient) Request(
// 	h *models.ServerHeader,
// 	serverId int64,
// 	req proto.Message,
// 	out proto.Message,
// ) *errmsg.ErrMsg {
// 	if h != nil {
// 		oldServerId, oldServerType := h.FromServerId, h.FromServerType
// 		h.FromServerId, h.FromServerType = this_.serverId, this_.serverType
// 		defer func() {
// 			h.FromServerId, h.FromServerType = oldServerId, oldServerType
// 		}()
// 	}
// 	size := protocol.NatsMarshalSize(h, req)
// 	d := bytespool.GetSample(size)
// 	defer bytespool.PutSample(d)
// 	d.Data = d.Data[:0]
// 	err := protocol.NatsMarshalTo(&d.Data, h, req)
// 	if err != nil {
// 		return err
// 	}
// 	msgName := string(proto.MessageName(req))
// 	if serverId != 0 {
// 		n := strings.IndexByte(msgName, '.')
// 		if n == -1 {
// 			return errmsg.NewProtocolErrorInfo(fmt.Sprintf("msgName is not a proto.MessageName:%s", msgName))
// 		}
//
// 		b := bytespool.GetSample(len(msgName) + 10)
// 		defer bytespool.PutSample(b)
// 		b.Data = b.Data[:0]
// 		b.Data = append(b.Data, msgName[:n]...)
// 		b.Data = append(b.Data, '.')
// 		b.Data = strconv.AppendInt(b.Data, serverId, 10)
// 		b.Data = append(b.Data, msgName[n:]...)
// 		msgName = *(*string)(unsafe.Pointer(&b.Data))
// 	}
// 	outMsg, e := this_.conn.Request(msgName, d.Data, time.Second*10)
// 	if e != nil {
// 		return errmsg.NewProtocolError(e)
// 	}
// 	err = protocol.NatsUnmarshalResponseWithout(outMsg.Data, out)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }
//
// func (this_ *NatsClient) RequestRaw(h *models.ServerHeader, serverId int64, msgName string, data []byte) (
// 	[]byte,
// 	*errmsg.ErrMsg,
// ) {
// 	if h != nil {
// 		oldServerId, oldServerType := h.FromServerId, h.FromServerType
// 		h.FromServerId, h.FromServerType = this_.serverId, this_.serverType
// 		defer func() {
// 			h.FromServerId, h.FromServerType = oldServerId, oldServerType
// 		}()
// 	}
// 	size := protocol.NatsMarshalDataSize(h, data)
// 	d := bytespool.GetSample(size)
// 	defer bytespool.PutSample(d)
// 	d.Data = d.Data[:0]
// 	err := protocol.NatsMarshalDataTo(&d.Data, h, data)
// 	if err != nil {
// 		return nil, err
// 	}
// 	if serverId != 0 {
// 		n := strings.IndexByte(msgName, '.')
// 		if n == -1 {
// 			return nil, errmsg.NewProtocolErrorInfo(fmt.Sprintf("msgName is not a proto.MessageName:%s", msgName))
// 		}
//
// 		b := bytespool.GetSample(len(msgName) + 10)
// 		defer bytespool.PutSample(b)
// 		b.Data = b.Data[:0]
// 		b.Data = append(b.Data, msgName[:n]...)
// 		b.Data = append(b.Data, '.')
// 		b.Data = strconv.AppendInt(b.Data, serverId, 10)
// 		b.Data = append(b.Data, msgName[n:]...)
// 		msgName = *(*string)(unsafe.Pointer(&b.Data))
// 	}
// 	outMsg, e := this_.conn.Request(msgName, d.Data, time.Second*10)
// 	if e != nil {
// 		return nil, errmsg.NewProtocolError(e)
// 	}
// 	return outMsg.Data, nil
// }

// Shutdown 关闭NATS
// func (this_ *NatsClient) Shutdown() {
// 	if atomic.CompareAndSwapInt32(&this_.closed, 0, 1) {
// 		_ = this_.conn.FlushTimeout(time.Second * 3)
// 		this_.conn.Close()
// 	}
// }
//
// // Subscribe 订阅主题
// func (this_ *NatsClient) Subscribe(subj string, h nats.MsgHandler) {
// 	if _, ok := this_.subs.Get(subj); ok {
// 		panic(fmt.Sprintf("subj [%s] had Subscribed", subj))
// 	}
// 	this_.log.Info("Subscribe", zap.String("urls", this_.urls), zap.String("subj", subj))
// 	sub, err := this_.conn.Subscribe(subj, h)
// 	utils.Must(err)
// 	this_.subs.Set(subj, sub)
// }
//
// type UserSubject struct {
// 	GameServerId int64
// 	RoleId       int64
// 	MsgName      string
// }
//
// func (s *UserSubject) CreateSubj(serverType common.ServerType) string {
// 	b := strings.Builder{}
// 	b.WriteString(serverType.String())
// 	b.WriteByte('/')
// 	b.WriteString(strconv.FormatInt(s.GameServerId, 10))
// 	b.WriteByte('/')
// 	b.WriteString(strconv.FormatInt(s.RoleId, 10))
// 	b.WriteString(".>")
// 	return b.String()
// }
//
// func (s *UserSubject) CreatePublish(serverType models.ServerType, b []byte) []byte {
// 	b = append(b, serverType.String()...)
// 	b = append(b, '/')
// 	b = strconv.AppendInt(b, s.GameServerId, 10)
// 	b = append(b, '/')
// 	b = strconv.AppendInt(b, s.RoleId, 10)
// 	b = append(b, '.')
// 	b = append(b, s.MsgName...)
// 	return b
// }
//
// func (s *UserSubject) Parse(str string) error {
// 	return ParseUserSubject(str, s)
// }
//
// func ParseUserSubject(str string, us *UserSubject) error {
// 	index := strings.IndexByte(str, '.')
// 	if index == -1 {
// 		return errors.New("Invalid UserSubject " + str)
// 	}
// 	header := str[:index]
// 	msgName := str[index+1:]
// 	index = strings.IndexByte(header, '/')
// 	if index == -1 {
// 		return errors.New("Invalid UserSubject " + str)
// 	}
// 	header = header[index+1:]
// 	index = strings.IndexByte(header, '/')
// 	if index == -1 {
// 		return errors.New("Invalid UserSubject " + str)
// 	}
// 	gameServerId, err := strconv.ParseInt(header[:index], 10, 64)
// 	if err != nil {
// 		return errors.New("Invalid UserSubject " + str)
// 	}
// 	RoleId, err := strconv.ParseInt(header[index+1:], 10, 64)
// 	if err != nil {
// 		return errors.New("Invalid UserSubject " + str)
// 	}
// 	us.MsgName = msgName
// 	us.GameServerId = gameServerId
// 	us.RoleId = RoleId
// 	return nil
// }
//
// // SubscribeUser 订阅某个用户
// func (this_ *NatsClient) SubscribeUser(serverType common.ServerType, gameServerId, roleId int64, queue chan *nats.Msg) {
// 	us := &UserSubject{
// 		GameServerId: gameServerId,
// 		RoleId:       roleId,
// 	}
// 	subj := us.CreateSubj(serverType)
// 	if _, ok := this_.subs.Get(subj); ok {
// 		return
// 	}
// 	this_.log.Info("QueueSubscribeUser", zap.String("subj", subj))
// 	sub, err := this_.conn.ChanSubscribe(subj, queue)
// 	utils.Must(err)
// 	this_.subs.Set(subj, sub)
// }
//
// // UnsubscribeUser 取消某个用户的订阅
// func (this_ *NatsClient) UnsubscribeUser(serverType common.ServerType, gameServerId, roleId int64) {
// 	us := &UserSubject{
// 		GameServerId: gameServerId,
// 		RoleId:       roleId,
// 	}
// 	subj := us.CreateSubj(serverType)
// 	if v, ok := this_.subs.Get(subj); ok {
// 		this_.subs.Remove(subj)
// 		safego.GOWithLogger(
// 			this_.log, func() {
// 				if v.IsValid() {
// 					err := v.Drain()
// 					if err != nil {
// 						this_.log.Warn("UnsubscribeUser Drain error", zap.String("subj", subj), zap.Error(err))
// 					}
// 				}
// 				for i := 0; i < 10; i++ {
// 					if !v.IsValid() {
// 						break
// 					}
// 				}
// 			},
// 		)
// 		this_.log.Info("QueueUnsubscribeUser", zap.String("subj", subj))
// 	}
// }
//
// // PublishUser 推送数据给某个用户
// func (this_ *NatsClient) PublishUser(
// 	serverType models.ServerType,
// 	gameServerId, roleId int64,
// 	msg proto.Message,
// ) *errmsg.ErrMsg {
// 	us := UserSubject{
// 		GameServerId: gameServerId,
// 		RoleId:       roleId,
// 		MsgName:      string(proto.MessageName(msg)),
// 	}
// 	b := bytespool.GetSample(256)
// 	defer bytespool.PutSample(b)
// 	b.Data = b.Data[:0]
// 	subjBytes := us.CreatePublish(serverType, b.Data)
// 	subj := utils.BytesToString(subjBytes)
// 	n := protocol.NatsMarshalResponseSize(msg)
// 	d := bytespool.GetSample(n)
// 	defer bytespool.PutSample(d)
// 	d.Data = d.Data[:0]
// 	err := protocol.NatsMarshalResponse(&d.Data, msg)
// 	if err != nil {
// 		return err
// 	}
// 	this_.log.Info(
// 		"PublishUser", zap.String("serverType", serverType.String()), zap.Int64("gameServerId", gameServerId),
// 		zap.Int64("roleId", roleId), zap.String("msgName", subj),
// 		zap.String("msgSize", utils.ByteCountIEC(int64(len(d.Data)))),
// 	)
// 	return errmsg.NewProtocolError(this_.conn.Publish(subj, d.Data))
// }
//
// // SubscribeHandler 订阅Topic
// func (this_ *NatsClient) SubscribeHandler(subj string, f func(*nats.Msg)) {
// 	if _, ok := this_.subs.Get(subj); ok {
// 		panic(fmt.Sprintf("subj [%s] had Subscribed", subj))
// 	}
// 	group := strings.ReplaceAll(subj, ">", "group")
// 	this_.log.Info(
// 		"SubscribeHandler", zap.String("urls", this_.urls), zap.String("subj", subj), zap.String("group", group),
// 	)
// 	cb := this_.f
// 	if f != nil {
// 		cb = f
// 	}
// 	sub, err := this_.conn.QueueSubscribe(subj, group, cb)
// 	utils.Must(err)
// 	this_.subs.Set(subj, sub)
// }
//
// // UnSub 解除订阅
// func (this_ *NatsClient) UnSub(subj string) {
// 	if s, ok := this_.subs.Get(subj); ok {
// 		this_.log.Info("Unsubscribe", zap.String("subj", subj))
// 		_ = s.Unsubscribe()
// 		this_.subs.Remove(subj)
// 	}
// }
//
// // SubscribeBroadcast 订阅广播主题
// func (this_ *NatsClient) SubscribeBroadcast(subj string, f func(*nats.Msg)) {
// 	if _, ok := this_.subs.Get(subj); ok {
// 		panic(fmt.Sprintf("subj [%s] had Subscribed", subj))
// 	}
// 	this_.log.Info("SubscribeBroadcast", zap.String("urls", this_.urls), zap.String("subj", subj))
// 	cb := this_.f
// 	if f != nil {
// 		cb = f
// 	}
// 	sub, err := this_.conn.Subscribe(subj, cb)
// 	utils.Must(err)
// 	this_.subs.Set(subj, sub)
// }
