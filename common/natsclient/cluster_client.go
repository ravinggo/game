package natsclient

import (
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/objectpool"
)

type ClusterClient struct {
	natsClients []*NatsClient
}

// NewClusterClient create a ClusterClient
// encapsulates the calling design of the entire framework
// param name Usually the server name ( baseenv.Config.ServerType )
// param urls a nats cluster urls
// param timeout for request
func NewClusterClient(name string, urls []string, timeout time.Duration) *ClusterClient {
	clusterClient := &ClusterClient{}
	clusterClient.natsClients = make([]*NatsClient, 0, len(urls))
	for _, url := range urls {
		natsClient := NewNatsClient(name, url, timeout)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}
	return clusterClient
}

// Close all NatsClient
func (this_ *ClusterClient) Close() {
	for _, natsClient := range this_.natsClients {
		natsClient.Close()
	}
}

// Shutdown all NatsClient
func (this_ *ClusterClient) Shutdown() {
	for _, natsClient := range this_.natsClients {
		natsClient.Shutdown()
	}
}

// SubscribeAll subscribe subj for all NatsClient if not subscribed.
func (this_ *ClusterClient) SubscribeAll(subj string, h nats.MsgHandler) {
	for _, natsClient := range this_.natsClients {
		natsClient.Subscribe(subj, h)
	}
}

// QueueSubscribeAll queue subscribe subj for all NatsClient if not subscribed.
func (this_ *ClusterClient) QueueSubscribeAll(subj string, h nats.MsgHandler) {
	for _, natsClient := range this_.natsClients {
		natsClient.QueueSubscribe(subj, h)
	}
}

// Unsubscribe unsubscribe subj for all NatsClient if subscribed.
func (this_ *ClusterClient) Unsubscribe(subj string) {
	for _, natsClient := range this_.natsClients {
		natsClient.Unsubscribe(subj)
	}
}

func (this_ *ClusterClient) getClient(c ctx.IContext) *NatsClient {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	if nsl == 1 {
		return this_.natsClients[0]
	}
	hashCtx, ok := c.(ctx.ToHash)
	if ok {
		return this_.natsClients[hashCtx.ToHash()%uint64(nsl)]
	}
	return this_.natsClients[rand.IntN(nsl)]
}

func (this_ *ClusterClient) getClientToServerId(c ctx.IContext, toServerId int64) *NatsClient {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	if nsl == 1 {
		return this_.natsClients[0]
	}
	hashCtx, ok := c.(ctx.ToHash)
	if ok {
		return this_.natsClients[hashCtx.ToHash()%uint64(nsl)]
	}
	if toServerId > 0 {
		return this_.natsClients[toServerId%int64(nsl)]
	}
	return this_.natsClients[rand.IntN(nsl)]
}

// ClusterPublish publish message with ClusterClient.
type ClusterPublish[T any, PUB define.ProtoMessagePtr[T]] struct {
	Pub    T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// NewClusterPublish create a ClusterPublish use objectpool
// ClusterPublish just can only be used once
func NewClusterPublish[T any, PUB define.ProtoMessagePtr[T]]() *ClusterPublish[T, PUB] {
	return objectpool.Get[ClusterPublish[T, PUB]]()
}

// Reset implement define.Clear
func (r *ClusterPublish[T, PUB]) Reset() {
	*r = ClusterPublish[T, PUB]{}
}

// Publish more info see ClusterClient.Publish
func (r *ClusterPublish[T, PUB]) Publish(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	err := cnc.Publish(c, (PUB)(&r.Pub))
	return err
}

// PublishToServer more info see ClusterClient.PublishToServer
func (r *ClusterPublish[T, PUB]) PublishToServer(cnc *ClusterClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	err := cnc.PublishToServer(c, toServerId, (PUB)(&r.Pub))
	return err
}

// Publish more info see NatsClient.Publish
// recommended use ClusterPublish.Publish
func (this_ *ClusterClient) Publish(c ctx.IContext, pubMsg proto.Message) *berror.ErrMsg {
	return this_.getClient(c).Publish(c, pubMsg)
}

// PublishToServer more info see NatsClient.PublishToServer
// recommended use ClusterPublish.PublishToServer
func (this_ *ClusterClient) PublishToServer(c ctx.IContext, toServerId int64, pubMsg proto.Message) *berror.ErrMsg {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.PublishToServer(c, toServerId, pubMsg)
}

// PublishRawData more info see NatsClient.PublishRawData
func (this_ *ClusterClient) PublishRawData(c ctx.IContext, toServerId int64, pubMsgName string, pubMsgData []byte) *berror.ErrMsg {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.PublishRawData(c, toServerId, pubMsgName, pubMsgData)
}

// Request rpc with ClusterClient.
// param reqMsg, respMsg escapes to heap.
// more info see NatsClient.Request
// recommended use ClusterRequest.Request
func (this_ *ClusterClient) Request(c ctx.IContext, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	return this_.getClient(c).Request(c, reqMsg, respMsg)
}

// ClusterRequest rpc with ClusterClient.
type ClusterRequest[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]] struct {
	Req    T
	Resp   T1
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// NewClusterRequest create a ClusterRequest use objectpool
// ClusterRequest just can only be used once
func NewClusterRequest[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]]() *ClusterRequest[T, T1, REQ, RESP] {
	c := objectpool.Get[ClusterRequest[T, T1, REQ, RESP]]()
	c.forNew = 1
	return c
}

// Reset implement define.Clear
func (r *ClusterRequest[T, T1, REQ, RESP]) Reset() {
	*r = ClusterRequest[T, T1, REQ, RESP]{}
}

// Request more info see ClusterClient.Request
func (r *ClusterRequest[T, T1, REQ, RESP]) Request(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("ClusterRequest is not created by NewClusterRequest")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return cnc.Request(c, (REQ)(&r.Req), (RESP)(&r.Resp))
	}
	panic("ClusterRequest is used")
}

// RequestToServer more info see ClusterClient.RequestToServer
func (r *ClusterRequest[T, T1, REQ, RESP]) RequestToServer(cnc *ClusterClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("ClusterRequest is not created by NewClusterRequest")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return cnc.RequestToServer(c, toServerId, (REQ)(&r.Req), (RESP)(&r.Resp))
	}
	panic("ClusterRequest is used")
}

// RequestToServer rpc with raw data for one NatsClient.
// param if toServerId==0, Receiver serverId is 0 or a cluster server node
// param reqMsg, respMsg escapes to heap.
// more info see NatsClient.RequestToServer
// recommended use ClusterRequest.RequestToServer
func (this_ *ClusterClient) RequestToServer(c ctx.IContext, toServerId int64, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.RequestToServer(c, toServerId, reqMsg, respMsg)
}

// RequestRaw rpc with raw data for one NatsClient.
// param if toServerId==0, Receiver serverId is 0 or a cluster server node
// param reqMsgName is proto message name.
// param reqMsgData is proto message data.
// return nats.Msg.Data and berror.ErrMsg.
func (this_ *ClusterClient) RequestRaw(c ctx.IContext, toServerId int64, reqMsgName string, reqMsgData []byte) ([]byte, *berror.ErrMsg) {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.RequestRaw(c, toServerId, reqMsgName, reqMsgData)
}

// ClusterSubscribeOneUser Generic Implementation : Subscribe user topic for one NatsClient.
// param us not escapes to heap.
// more info see ClusterClient.SubscribeOneUser
func ClusterSubscribeOneUser[US UserSubjectPtr[T], T any](cnc *ClusterClient, us US, handler nats.MsgHandler) {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}
	client := cnc.natsClients[us.ToHash()%nsl]
	ClientSubscribeUser(client, us, handler)
}

// ClusterSubscribeAllUser Generic Implementation : Subscribe user topic for all NatsClient.
// param us not escapes to heap.
// more info see ClusterClient.SubscribeUserAllUser
func ClusterSubscribeAllUser[US UserSubjectPtr[T], T any](cnc *ClusterClient, us US, handler nats.MsgHandler) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		ClientSubscribeUser(client, us, handler)
	}
}

// ClusterUnsubscribeUser Generic Implementation : Unsubscribe user topic for all NatsClient if subscribed.
// param us not escapes to heap.
func ClusterUnsubscribeUser[US UserSubjectPtr[T], T any](cnc *ClusterClient, us US) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		ClientUnsubscribeUser(client, us)
	}
}

// SubscribeOneUser Subscribe user topic for one NatsClient.
// param us escapes to heap.
// recommended use ClusterSubscribeOneUser
func (this_ *ClusterClient) SubscribeOneUser(
	us UserSubject, handler nats.MsgHandler,
) {
	nsl := uint64(len(this_.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%nsl]
	client.SubscribeUser(us, handler)
}

// SubscribeUserAllUser Subscribe user topic for all NatsClient.
// param us escapes to heap.
// recommended use ClusterSubscribeAllUser
func (this_ *ClusterClient) SubscribeUserAllUser(
	us UserSubject, handler nats.MsgHandler,
) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range this_.natsClients {
		client.SubscribeUser(us, handler)
	}
}

// UnsubscribeUser Unsubscribe user topic for all NatsClient if subscribed.
// param us escapes to heap.
// recommended use ClusterUnsubscribeUser
func (this_ *ClusterClient) UnsubscribeUser(us UserSubject) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range this_.natsClients {
		client.UnsubscribeUser(us)
	}
}

// PublishUser Publish msg user topic for one NatsClient.
// param us,pubMsg escapes to heap.
// recommended use ClusterPublishUser
func (this_ *ClusterClient) PublishUser(c ctx.IContext, us UserSubject, pubMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(this_.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%nsl]
	return client.PublishUser(c, us, pubMsg)
}

// RequestUser Request msg user topic for one NatsClient.
// param us, reqMsg, respMsg escapes to heap.
// recommended use ClusterRequestUser
func (this_ *ClusterClient) RequestUser(c ctx.IContext, us UserSubject, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(this_.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%nsl]
	return client.RequestUser(c, us, reqMsg, respMsg)
}

// ClusterPublishUser Generic Implementation : publish user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see ClusterClient.PublishUser
// create use NewPublishUser
type ClusterPublishUser[T, T1 any, US UserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]] struct {
	Pub    T1
	Us     T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClusterPublishUser[T, T1, US, PUB]) Reset() {
	*r = ClusterPublishUser[T, T1, US, PUB]{}
}

// NewClusterPublishUser create ClusterPublishUser use objectpool
func NewClusterPublishUser[T, T1 any, US UserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]]() *ClusterPublishUser[T, T1, US, PUB] {
	c := objectpool.Get[ClusterPublishUser[T, T1, US, PUB]]()
	c.forNew = 1
	return c
}

// Publish more info see ClusterClient.PublishUser
func (r *ClusterPublishUser[T, T1, US, PUB]) Publish(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClusterPublishUser please use NewClusterPublishUser")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return cnc.PublishUser(c, (US)(&r.Us), (PUB)(&r.Pub))
	}
	panic("ClusterPublishUser used")
}

// ClusterRequestUser Generic Implementation : rpc user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see ClusterClient.RequestUser
// create use NewPublishUser
type ClusterRequestUser[T, T1, T2 any, US UserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]] struct {
	Req    T1
	Resp   T2
	Us     T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClusterRequestUser[T, T1, T2, US, REQ, RESP]) Reset() {
	*r = ClusterRequestUser[T, T1, T2, US, REQ, RESP]{}
}

// NewClusterRequestUser create ClusterRequestUser use objectpool
func NewClusterRequestUser[T, T1, T2 any, US UserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]](
) *ClusterRequestUser[T, T1, T2, US, REQ, RESP] {
	c := objectpool.Get[ClusterRequestUser[T, T1, T2, US, REQ, RESP]]()
	c.forNew = 1
	return c
}

// Request more info see ClusterClient.Request
func (r *ClusterRequestUser[T, T1, T2, US, REQ, RESP]) Request(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClusterRequestUser please use NewClusterRequestUser")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return cnc.RequestUser(c, (US)(&r.Us), (REQ)(&r.Req), (RESP)(&r.Resp))
	}
	panic("ClusterRequestUser used")
}
