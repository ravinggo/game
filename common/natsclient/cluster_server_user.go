package natsclient

import (
	"sync/atomic"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/objectpool"
)

// ClusterSubscribeOneServerUser Generic Implementation : Subscribe user topic for one NatsClient.
// param us not escapes to heap.
// more info see ClusterClient.SubscribeOneServerUser
func ClusterSubscribeOneServerUser[US ServerUserSubjectPtr[T], T any](cnc *ClusterClient, us US, handler nats.MsgHandler) {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}
	client := cnc.natsClients[us.ToHash()%nsl]
	ClientSubscribeServerUser(client, us, handler)
}

// ClusterSubscribeAllServerUser Generic Implementation : Subscribe user topic for all NatsClient.
// param us not escapes to heap.
// more info see ClusterClient.SubscribeUserAllServerUser
func ClusterSubscribeAllServerUser[US ServerUserSubjectPtr[T], T any](cnc *ClusterClient, us US, handler nats.MsgHandler) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		ClientSubscribeServerUser(client, us, handler)
	}
}

// ClusterUnsubscribeServerUser Generic Implementation : Unsubscribe user topic for all NatsClient if subscribed.
// param us not escapes to heap.
func ClusterUnsubscribeServerUser[US ServerUserSubjectPtr[T], T any](cnc *ClusterClient, us US) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		ClientUnsubscribeServerUser(client, us)
	}
}

// SubscribeOneServerUser Subscribe user topic for one NatsClient.
// param us escapes to heap.
// recommended use ClusterSubscribeOneServerUser
func (this_ *ClusterClient) SubscribeOneServerUser(
	us ServerUserSubject, handler nats.MsgHandler,
) {
	nsl := uint64(len(this_.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%nsl]
	client.SubscribeServerUser(us, handler)
}

// SubscribeUserAllServerUser Subscribe user topic for all NatsClient.
// param us escapes to heap.
// recommended use ClusterSubscribeAllServerUser
func (this_ *ClusterClient) SubscribeUserAllServerUser(
	us ServerUserSubject, handler nats.MsgHandler,
) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range this_.natsClients {
		client.SubscribeServerUser(us, handler)
	}
}

// UnsubscribeServerUser Unsubscribe user topic for all NatsClient if subscribed.
// param us escapes to heap.
// recommended use ClusterUnsubscribeServerUser
func (this_ *ClusterClient) UnsubscribeServerUser(us ServerUserSubject) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range this_.natsClients {
		client.UnsubscribeServerUser(us)
	}
}

// PublishServerUser Publish msg user topic for one NatsClient.
// param us,pubMsg escapes to heap.
// recommended use ClusterPublishServerUser
func (this_ *ClusterClient) PublishServerUser(c ctx.IContext, us ServerUserSubject, pubMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(this_.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%nsl]
	return client.PublishServerUser(c, us, pubMsg)
}

// RequestServerUser Request msg user topic for one NatsClient.
// param us, reqMsg, respMsg escapes to heap.
// recommended use ClusterRequestServerUser
func (this_ *ClusterClient) RequestServerUser(c ctx.IContext, us ServerUserSubject, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(this_.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[us.ToHash()%nsl]
	return client.RequestServerUser(c, us, reqMsg, respMsg)
}

// ClusterPublishServerUser Generic Implementation : publish user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see ClusterClient.PublishServerUser
// create use NewPublishUser
type ClusterPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]] struct {
	Pub    T1
	Us     T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClusterPublishServerUser[T, T1, US, PUB]) Reset() {
	*r = ClusterPublishServerUser[T, T1, US, PUB]{}
}

// NewClusterPublishServerUser create ClusterPublishServerUser use objectpool
func NewClusterPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]]() *ClusterPublishServerUser[T, T1, US, PUB] {
	c := objectpool.Get[ClusterPublishServerUser[T, T1, US, PUB]]()
	c.forNew = 1
	return c
}

// Publish more info see ClusterClient.PublishServerUser
func (r *ClusterPublishServerUser[T, T1, US, PUB]) Publish(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClusterPublishServerUser please use NewClusterPublishServerUser")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return cnc.PublishServerUser(c, (US)(&r.Us), (PUB)(&r.Pub))
	}
	panic("ClusterPublishServerUser used")
}

// ClusterRequestServerUser Generic Implementation : rpc user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see ClusterClient.RequestServerUser
// create use NewPublishUser
type ClusterRequestServerUser[T, T1, T2 any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]] struct {
	Req    T1
	Resp   T2
	Us     T
	used   uint32
	forNew uint32
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClusterRequestServerUser[T, T1, T2, US, REQ, RESP]) Reset() {
	*r = ClusterRequestServerUser[T, T1, T2, US, REQ, RESP]{}
}

// NewClusterRequestServerUser create ClusterRequestServerUser use objectpool
func NewClusterRequestServerUser[T, T1, T2 any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]](
) *ClusterRequestServerUser[T, T1, T2, US, REQ, RESP] {
	c := objectpool.Get[ClusterRequestServerUser[T, T1, T2, US, REQ, RESP]]()
	c.forNew = 1
	return c
}

// Request more info see ClusterClient.Request
func (r *ClusterRequestServerUser[T, T1, T2, US, REQ, RESP]) Request(cnc *ClusterClient, c ctx.IContext) *berror.ErrMsg {
	if r.forNew != 1 {
		panic("create ClusterRequestServerUser please use NewClusterRequestServerUser")
	}
	if atomic.CompareAndSwapUint32(&r.used, 0, 1) {
		defer objectpool.Put(r)
		return cnc.RequestServerUser(c, (US)(&r.Us), (REQ)(&r.Req), (RESP)(&r.Resp))
	}
	panic("ClusterRequestServerUser used")
}
