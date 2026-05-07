package natsclient

import (
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/safego"
)

// ClusterClientServerUser is a ClusterClient with user topic
type ClusterClientServerUser[T any, US ServerUserSubjectPtr[T]] struct {
	*ClusterClient
	natsClients    []*ServerUserNatsClient[T, US]
	queues         []chan *nats.Msg
	userHandler    nats.MsgHandler
	closeCh        chan struct{}
	isCreateQueues bool
}

// NewClusterClientServerUser create a ClusterClientServerUser.
func NewClusterClientServerUser[T any, US ServerUserSubjectPtr[T]](
	urls []string, rpcTimeout time.Duration, userHandler nats.MsgHandler, opts ...UNOption,
) *ClusterClientServerUser[T, US] {
	if userHandler == nil {
		panic("handler is nil")
	}
	uh := func(msg *nats.Msg) {
		defer safego.Recover()
		if strings.HasSuffix(msg.Subject, waitSuccessCheckStr) && msg.Reply != "" && len(msg.Data) == 0 {
			err := msg.Respond(nil)
			if err != nil {
				logger.Log.Error().Err(err).Str("msgName", msg.Subject).Msg("deal wait success error")
			}
			return
		} else if !strings.HasSuffix(msg.Subject, normalCheckStr) {
			// invalid msg
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

	cnc := NewClusterClient(urls, rpcTimeout, un.opts...)
	clusterClient := &ClusterClientServerUser[T, US]{
		ClusterClient:  cnc,
		queues:         queues,
		userHandler:    uh,
		closeCh:        make(chan struct{}, len(queues)),
		isCreateQueues: isCreateQueues,
	}
	clusterClient.natsClients = make([]*ServerUserNatsClient[T, US], 0, len(cnc.natsClients))
	for _, c := range cnc.natsClients {
		natsClient := newServerUserNatsClientForCluster[T, US](c, queues)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}

	clusterClient.startUserChan()
	return clusterClient
}

func NewClusterClientServerUser2[T any, US ServerUserSubjectPtr[T]](
	cnc *ClusterClient, userHandler nats.MsgHandler, opts ...UNOption,
) *ClusterClientServerUser[T, US] {

	if userHandler == nil {
		panic("handler is nil")
	}
	uh := func(msg *nats.Msg) {
		defer safego.Recover()
		if strings.HasSuffix(msg.Subject, waitSuccessCheckStr) && msg.Reply != "" && len(msg.Data) == 0 {
			err := msg.Respond(nil)
			if err != nil {
				logger.Log.Error().Err(err).Str("msgName", msg.Subject).Msg("deal wait success error")
			}
			return
		} else if !strings.HasSuffix(msg.Subject, normalCheckStr) {
			// invalid msg
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

	clusterClient := &ClusterClientServerUser[T, US]{
		ClusterClient:  cnc,
		queues:         queues,
		userHandler:    uh,
		closeCh:        make(chan struct{}, len(queues)),
		isCreateQueues: isCreateQueues,
	}
	clusterClient.natsClients = make([]*ServerUserNatsClient[T, US], 0, len(cnc.natsClients))
	for _, c := range cnc.natsClients {
		natsClient := newServerUserNatsClientForCluster[T, US](c, queues)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}

	clusterClient.startUserChan()
	return clusterClient
}

func (cnc *ClusterClientServerUser[T, US]) Close() {
	for _, nc := range cnc.natsClients {
		nc.Close()
	}

	if cnc.isCreateQueues {
		for _, q := range cnc.queues {
			close(q)
		}
		for range len(cnc.queues) {
			<-cnc.closeCh
		}
	}
}

func (cnc *ClusterClientServerUser[T, US]) startUserChan() {
	for _, ch := range cnc.queues {
		go func(ch chan *nats.Msg) {
			for msg := range ch {
				cnc.userHandler(msg)
			}
			cnc.closeCh <- struct{}{}
		}(ch)
	}
}

// UserSubscribeOne  Subscribe user topic for one NatsClient.
// param us not escapes to heap.
func (cnc *ClusterClientServerUser[T, US]) UserSubscribeOne(us US) {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}
	client := cnc.natsClients[uint64(us.GetRoleID())%nsl]
	client.SubscribeUser(us)
}

// UserSubscribeOneWaitSuccess  Subscribe user topic for one NatsClient and wait success.
func (cnc *ClusterClientServerUser[T, US]) UserSubscribeOneWaitSuccess(us US) {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}
	client := cnc.natsClients[uint64(us.GetRoleID())%nsl]
	client.SubscribeUserWaitSuccess(us)
}

// UserSubscribeAll Subscribe user topic for all NatsClient.
// param us not escapes to heap.
func (cnc *ClusterClientServerUser[T, US]) UserSubscribeAll(us US) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		client.SubscribeUser(us)
	}
}

// UserSubscribeAllWaitSuccess Subscribe user topic for all NatsClient and wait success.
func (cnc *ClusterClientServerUser[T, US]) UserSubscribeAllWaitSuccess(us US) {
	nsl := len(cnc.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	for _, client := range cnc.natsClients {
		client.SubscribeUserWaitSuccess(us)
	}
}

// UserUnsubscribe  Unsubscribe user topic for all NatsClient if subscribed.
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

	client := cnc.natsClients[uint64(us.GetRoleID())%nsl]
	return client.PublishUser(c, us, pubMsg)
}

func (cnc *ClusterClientServerUser[T, US]) PublishUserMany(c ctx.IContext, us US, pubMsg ...proto.Message) *berror.ErrMsg {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := cnc.natsClients[uint64(us.GetRoleID())%nsl]
	return client.PublishUserMany(c, us, pubMsg...)
}

// RequestUser Request msg user topic for one NatsClient.
// param us, reqMsg, respMsg escapes to heap.
// recommended use ClusterRequestServerUser
func (cnc *ClusterClientServerUser[T, US]) RequestUser(c ctx.IContext, us US, reqMsg proto.Message, respMsg proto.Message) *berror.ErrMsg {
	nsl := uint64(len(cnc.natsClients))
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := cnc.natsClients[uint64(us.GetRoleID())%nsl]
	return client.RequestUser(c, us, reqMsg, respMsg)
}

// ClusterPublishServerUser publish msg to user topic
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

// ClusterRequestServerUser rpc user topic
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
func NewClusterRequestServerUser[T1, T2, T any, US ServerUserSubjectPtr[T], REQ define.ProtoMessagePtr[T1],
RESP define.ProtoMessagePtr[T2]]() *ClusterRequestServerUser[T1, T2, T, US, REQ, RESP] {
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
