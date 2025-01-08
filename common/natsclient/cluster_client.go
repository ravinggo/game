package natsclient

import (
	"math/rand/v2"
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

func NewClusterClient(name string, urls []string, timeout time.Duration) *ClusterClient {
	clusterClient := &ClusterClient{}
	clusterClient.natsClients = make([]*NatsClient, 0, len(urls))
	for _, url := range urls {
		natsClient := NewNatsClient(name, url, timeout)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}
	return clusterClient
}

func (this_ *ClusterClient) Close() {
	for _, natsClient := range this_.natsClients {
		natsClient.Close()
	}
}

func (this_ *ClusterClient) Shutdown() {
	for _, natsClient := range this_.natsClients {
		natsClient.Shutdown()
	}
}

func (this_ *ClusterClient) SubscribeAll(subj string, h nats.MsgHandler) {
	for _, natsClient := range this_.natsClients {
		natsClient.Subscribe(subj, h)
	}
}

func (this_ *ClusterClient) QueueSubscribeAll(subj string, h nats.MsgHandler) {
	for _, natsClient := range this_.natsClients {
		natsClient.QueueSubscribe(subj, h)
	}
}

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

type ClusterPublish[T any, PUB define.ProtoMessagePtr[T]] struct {
	Pub T
}

func NewClusterPublish[T any, PUB define.ProtoMessagePtr[T]]() *ClusterPublish[T, PUB] {
	return objectpool.Get[ClusterPublish[T, PUB]]()
}

func (r *ClusterPublish[T, PUB]) Reset() {
	proto.Reset((PUB)(&r.Pub))
}

func (r *ClusterPublish[T, PUB]) Publish(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	err := cnc.Publish(c, (PUB)(&r.Pub))
	return err
}

func (r *ClusterPublish[T, PUB]) PublishToServer(cnc *ClusterClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	err := cnc.PublishToServer(c, toServerId, (PUB)(&r.Pub))
	return err
}

func (this_ *ClusterClient) Publish(c ctx.IContext, pubMsg proto.Message) *berror.ErrMsg {
	return this_.getClient(c).Publish(c, pubMsg)
}

func (this_ *ClusterClient) PublishToServer(c ctx.IContext, toServerId int64, pubMsg proto.Message) *berror.ErrMsg {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.PublishToServer(c, toServerId, pubMsg)
}

func (this_ *ClusterClient) PublishRawData(c ctx.IContext, toServerId int64, pubMsgName string, pubMsgData []byte) *berror.ErrMsg {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.PublishRawData(c, toServerId, pubMsgName, pubMsgData)
}

func (this_ *ClusterClient) Request(c ctx.IContext, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	return this_.getClient(c).Request(c, reqMsg, respMsg)
}

type ClusterRequest[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]] struct {
	Req  T
	Resp T1
}

func NewClusterRequest[T, T1 any, REQ define.ProtoMessagePtr[T], RESP define.ProtoMessagePtr[T1]]() *ClusterRequest[T, T1, REQ, RESP] {
	return objectpool.Get[ClusterRequest[T, T1, REQ, RESP]]()
}

func (r *ClusterRequest[T, T1, REQ, RESP]) Reset() {
	proto.Reset((REQ)(&r.Req))
	proto.Reset((RESP)(&r.Resp))
}

func (r *ClusterRequest[T, T1, REQ, RESP]) Request(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	err := cnc.Request(c, (REQ)(&r.Req), (RESP)(&r.Resp))
	return err
}

func (r *ClusterRequest[T, T1, REQ, RESP]) RequestToServer(cnc *ClusterClient, c ctx.IContext, toServerId int64) *berror.ErrMsg {
	err := cnc.RequestToServer(c, toServerId, (REQ)(&r.Req), (RESP)(&r.Resp))
	return err
}

func (this_ *ClusterClient) RequestToServer(c ctx.IContext, toServerId int64, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.RequestToServer(c, toServerId, reqMsg, respMsg)
}

func (this_ *ClusterClient) RequestRaw(c ctx.IContext, toServerId int64, reqMsgName string, reqMsgData []byte) ([]byte, *berror.ErrMsg) {
	nc := this_.getClientToServerId(c, toServerId)
	return nc.RequestRaw(c, toServerId, reqMsgName, reqMsgData)
}

func (this_ *ClusterClient) SubscribeOneUser(
	us UserSubject, queue chan *nats.Msg,
) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%int64(nsl)]
	client.SubscribeUser(us, queue)
}

func (this_ *ClusterClient) SubscribeUserAll(
	us UserSubject, queue chan *nats.Msg,
) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range this_.natsClients {
		client.SubscribeUser(us, queue)
	}
}

func (this_ *ClusterClient) UnsubscribeOneUser(us UserSubject) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%int64(nsl)]
	client.UnsubscribeUser(us)
}

func (this_ *ClusterClient) UnsubscribeAllUser(us UserSubject) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range this_.natsClients {
		client.UnsubscribeUser(us)
	}
}

func (this_ *ClusterClient) PublishUser(c ctx.IContext, us UserSubject, msg proto.Message) *berror.ErrMsg {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%int64(nsl)]
	return client.PublishUser(c, us, msg)
}

func (this_ *ClusterClient) RequestUser(c ctx.IContext, us UserSubject, msg proto.Message, out proto.Message) *berror.ErrMsg {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%int64(nsl)]
	return client.RequestUser(c, us, msg, out)
}

type ClusterRequestUser[T any] struct {
	Ret T
	US  UserSubject
}

func (r *ClusterRequestUser[T]) Request(cnc *ClusterClient, c ctx.IContext, msg proto.Message) *berror.ErrMsg {
	var a any = &r.Ret
	err := cnc.RequestUser(c, r.US, msg, a.(proto.Message))
	return err
}
