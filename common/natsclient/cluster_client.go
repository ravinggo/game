package natsclient

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"math/rand"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/logger"
)

type ClusterClient struct {
	natsClients []*NatsClient
}

var natsTracer = otel.Tracer("NatsClient")

func NewClusterClient(serverType models.ServerType, serverId int64, urls []string, log *logger.Logger) *ClusterClient {
	clusterClient := &ClusterClient{}
	clusterClient.natsClients = make([]*NatsClient, 0, len(urls))
	for _, url := range urls {
		natsClient := NewNatsClient(serverType, serverId, url, log)
		clusterClient.natsClients = append(clusterClient.natsClients, natsClient)
	}
	return clusterClient
}

func (this_ *ClusterClient) SubscribeUser(
	serverType models.ServerType,
	gameServerId, roleId int64,
	queue chan *nats.Msg,
) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}

	client := this_.natsClients[roleId%int64(nsl)]
	client.SubscribeUser(serverType, gameServerId, roleId, queue)
}

func (this_ *ClusterClient) RequestWithCtx(c *ctx.Context, serverId int64, req, out proto.Message) *errmsg.ErrMsg {
	var h *models.ServerHeader
	if c != nil {
		h = &c.ServerHeader
	}
	nc := this_.getNatsClientWithHeader(h, serverId)
	return nc.RequestWithCtx(c, serverId, req, out)
}

func (this_ *ClusterClient) Request(
	h *models.ServerHeader,
	serverId int64,
	req proto.Message,
	out proto.Message,
) *errmsg.ErrMsg {
	nc := this_.getNatsClientWithHeader(h, serverId)
	return nc.Request(h, serverId, req, out)
}

func (this_ *ClusterClient) RequestRaw(h *models.ServerHeader, serverId int64, msgName string, data []byte) (
	[]byte,
	*errmsg.ErrMsg,
) {
	nc := this_.getNatsClientWithHeader(h, serverId)
	return nc.RequestRaw(h, serverId, msgName, data)
}

func (this_ *ClusterClient) PublishUser(
	serverType models.ServerType,
	gameServerId, roleId int64,
	msg proto.Message,
) *errmsg.ErrMsg {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	client := this_.natsClients[roleId%int64(nsl)]
	return client.PublishUser(serverType, gameServerId, roleId, msg)
}

func (this_ *ClusterClient) UnsubscribeUser(serverType models.ServerType, gameServerId, roleId int64) {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	client := this_.natsClients[roleId%int64(nsl)]
	client.UnsubscribeUser(serverType, gameServerId, roleId)
}

func (this_ *ClusterClient) Close() {
	for _, natsClient := range this_.natsClients {
		natsClient.Close()
	}
}

func (this_ *ClusterClient) PublishCtx(c *ctx.Context, serverId int64, msg proto.Message) *errmsg.ErrMsg {
	if global_env.GetConfig().OpenTraceing && c != nil {
		var span trace.Span
		c.Context, span = natsTracer.Start(c.Context, "PublishCtx")
		defer span.End()
		span.SetAttributes(attribute.String("name", string(proto.MessageName(msg))))
	}
	var header *models.ServerHeader
	if c != nil {
		header = &c.ServerHeader
	}
	nc := this_.getNatsClientWithHeader(header, serverId)
	return nc.Publish(serverId, header, msg)
}

func (this_ *ClusterClient) Publish(serverId int64, h *models.ServerHeader, msg proto.Message) *errmsg.ErrMsg {
	if global_env.GetConfig().OpenTraceing {
		var span trace.Span
		_, span = natsTracer.Start(context.Background(), "Publish")
		defer span.End()
		span.SetAttributes(attribute.String("name", string(proto.MessageName(msg))))
	}
	nc := this_.getNatsClientWithHeader(h, serverId)
	return nc.Publish(serverId, h, msg)
}

func (this_ *ClusterClient) PublishRawData(
	serverId int64,
	h *models.ServerHeader,
	msgName string,
	msgData []byte,
) *errmsg.ErrMsg {
	if global_env.GetConfig().OpenTraceing {
		var span trace.Span
		_, span = natsTracer.Start(context.Background(), "PublishRawData")
		defer span.End()
		span.SetAttributes(attribute.String("name", msgName))
	}
	nc := this_.getNatsClientWithHeader(h, serverId)
	return nc.PublishRawData(serverId, h, msgName, msgData)
}

func (this_ *ClusterClient) Shutdown() {
	for _, natsClient := range this_.natsClients {
		natsClient.Shutdown()
	}
}

func (this_ *ClusterClient) Subscribe(subj string, h nats.MsgHandler) {
	for _, natsClient := range this_.natsClients {
		natsClient.Subscribe(subj, h)
	}
}

func (this_ *ClusterClient) SubscribeHandler(subj string, f func(*nats.Msg)) {
	for _, natsClient := range this_.natsClients {
		natsClient.SubscribeHandler(subj, f)
	}
}

func (this_ *ClusterClient) UnSub(subj string) {
	for _, natsClient := range this_.natsClients {
		natsClient.UnSub(subj)
	}
}

func (this_ *ClusterClient) SubscribeBroadcast(subj string, f func(*nats.Msg)) {
	for _, natsClient := range this_.natsClients {
		natsClient.SubscribeBroadcast(subj, f)
	}
}

func (this_ *ClusterClient) getNatsClientWithHeader(sh *models.ServerHeader, serverId int64) *NatsClient {
	nsl := len(this_.natsClients)
	if nsl == 0 {
		panic("nats client is empty")
	}
	if nsl == 1 {
		return this_.natsClients[0]
	}
	if sh != nil {
		if sh.RoleId != 0 {
			return this_.natsClients[sh.RoleId%int64(nsl)]
		}

		if sh.FromServerId > 0 {
			temp := bytespool.GetSample(8)
			binary.LittleEndian.PutUint64(temp.Data, uint64(sh.FromServerId))
			nc := this_.natsClients[crc32.ChecksumIEEE(temp.Data)%uint32(nsl)]
			bytespool.PutSample(temp)
			return nc
		}
	}
	if serverId > 0 {
		temp := bytespool.GetSample(8)
		binary.LittleEndian.PutUint64(temp.Data, uint64(serverId))
		nc := this_.natsClients[crc32.ChecksumIEEE(temp.Data)%uint32(nsl)]
		bytespool.PutSample(temp)
		return nc
	}
	return this_.natsClients[rand.Intn(nsl)]
}
