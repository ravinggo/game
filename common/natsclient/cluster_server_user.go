package natsclient

import (
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/objectpool"
)

type ClusterClientServerUser[T any, US ServerUserSubjectPtr[T]] struct {
	*ClusterClient
	natsClients []*ServerUserNatsClient[T, US]
}

func NewClusterClientServerUser[T any, US ServerUserSubjectPtr[T]](
	name string, urls []string, timeout time.Duration,
) *ClusterClientServerUser[T, US] {
	cnc := NewClusterClient(name, urls, timeout)

	clusterClient := &ClusterClientServerUser[T, US]{ClusterClient: cnc}
	clusterClient.natsClients = make([]*ServerUserNatsClient[T, US], 0, len(cnc.natsClients))
	for _, c := range cnc.natsClients {
		natsClient := newServerUserNatsClient[T, US](c)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}
	return clusterClient
}

func NewClusterClientServerUser2[T any, US ServerUserSubjectPtr[T]](
	cnc *ClusterClient,
) *ClusterClientServerUser[T, US] {

	clusterClient := &ClusterClientServerUser[T, US]{}
	clusterClient.natsClients = make([]*ServerUserNatsClient[T, US], 0, len(cnc.natsClients))
	for _, c := range cnc.natsClients {
		natsClient := newServerUserNatsClient[T, US](c)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}
	return clusterClient
}

// UserSubscribeOne Generic Implementation : Subscribe user topic for one NatsClient.
// param us not escapes to heap.
func (cnc *ClusterClientServerUser[T, US]) UserSubscribeOne(us US, handler nats.MsgHandler) {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}
	hash := us.ToHash()
	client := cnc.natsClients[hash%nsl]
	client.SubscribeUser(us, handler)
}

func (cnc *ClusterClientServerUser[T, US]) UserSubscribeOneWaitSuccess(us US, handler nats.MsgHandler) {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}
	hash := us.ToHash()
	client := cnc.natsClients[hash%nsl]
	client.SubscribeUserWaitSuccess(us, handler)
}

// UserSubscribeAll Generic Implementation : Subscribe user topic for all NatsClient.
// param us not escapes to heap.
func (cnc *ClusterClientServerUser[T, US]) UserSubscribeAll(us US, handler nats.MsgHandler) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		client.SubscribeUser(us, handler)
	}
}

func (cnc *ClusterClientServerUser[T, US]) UserSubscribeAllWaitSuccess(us US, handler nats.MsgHandler) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		client.SubscribeUserWaitSuccess(us, handler)
	}
}

// UserUnsubscribe Generic Implementation : Unsubscribe user topic for all NatsClient if subscribed.
// param us not escapes to heap.
func (cnc *ClusterClientServerUser[T, US]) UserUnsubscribe(us US) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		client.UnsubscribeUser(us)
	}
}

// PublishUser Publish msg user topic for one NatsClient.
// param us,pubMsg escapes to heap.
// recommended use ClusterPublishServerUser
func (cnc *ClusterClientServerUser[T, US]) PublishUser(c ctx.IContext, us US, pubMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := cnc.natsClients[us.ToHash()%nsl]
	return client.PublishUser(c, us, pubMsg)
}

// RequestUser Request msg user topic for one NatsClient.
// param us, reqMsg, respMsg escapes to heap.
// recommended use ClusterRequestServerUser
func (cnc *ClusterClientServerUser[T, US]) RequestUser(c ctx.IContext, us US, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := cnc.natsClients[us.ToHash()%nsl]
	return client.RequestUser(c, us, reqMsg, respMsg)
}

// ClusterPublishServerUser Generic Implementation : publish user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see ClusterClientServerUser.PublishUser
// create use NewPublishUser
type ClusterPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]] struct {
	Pub T1
	Us  T
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClusterPublishServerUser[T, T1, US, PUB]) Reset() {
	*r = ClusterPublishServerUser[T, T1, US, PUB]{}
}

// NewClusterPublishServerUser create ClusterPublishServerUser use objectpool
func NewClusterPublishServerUser[T, T1 any, US ServerUserSubjectPtr[T], PUB define.ProtoMessagePtr[T1]]() *ClusterPublishServerUser[T, T1, US, PUB] {
	c := objectpool.Get[ClusterPublishServerUser[T, T1, US, PUB]]()
	return c
}

// Publish more info see ClusterClientServerUser.PublishUser
func (r *ClusterPublishServerUser[T, T1, US, PUB]) Publish(cnc *ClusterClientServerUser[T, US], c ctx.IContext) *berror.ErrMsg {
	err := cnc.PublishUser(c, (US)(&r.Us), (PUB)(&r.Pub))
	return err
}

func (r *ClusterPublishServerUser[T, T1, US, PUB]) Free() {
	objectpool.Put(r)
}

// ClusterRequestServerUser Generic Implementation : rpc user topic
// designed to use objectpool, because Pub,Us will always escape to heap
// for more information, see ClusterClientServerUser.RequestUser
// create use NewPublishUser
type ClusterRequestServerUser[T1, T2, T any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]] struct {
	Req  T1
	Resp T2
	Us   T
	define.DoNotCopy
}

// Reset implement define.Clear
func (r *ClusterRequestServerUser[T1, T2, T, US, REQ, RESP]) Reset() {
	*r = ClusterRequestServerUser[T1, T2, T, US, REQ, RESP]{}
}

// NewClusterRequestServerUser create ClusterRequestServerUser use objectpool
func NewClusterRequestServerUser[T1, T2, T any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2]](
) *ClusterRequestServerUser[T1, T2, T, US, REQ, RESP] {
	c := objectpool.Get[ClusterRequestServerUser[T1, T2, T, US, REQ, RESP]]()
	return c
}

// Request more info see ClusterClientServerUser.RequestUser
func (r *ClusterRequestServerUser[T1, T2, T, US, REQ, RESP]) Request(
	cnc *ClusterClientServerUser[T, US],
	c ctx.IContext,
) *berror.ErrMsg {
	return cnc.RequestUser(c, (US)(&r.Us), (REQ)(&r.Req), (RESP)(&r.Resp))
}

func (r *ClusterRequestServerUser[T1, T2, T, US, REQ, RESP]) Free() {
	objectpool.Put(r)
}
