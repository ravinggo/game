package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/service"
)

type TestService struct {
	svc *service.ServerUserService[natsclient.ServerIntUserSubject, ctx.Int64TraceCtx, *ctx.Int64TraceCtx, *natsclient.ServerIntUserSubject]
}

func NewTestService() *TestService {
	t := &TestService{
		svc: service.NewServerUserService[natsclient.ServerIntUserSubject, ctx.Int64TraceCtx](
			[]string{"nats://192.168.0.166:4222"},
			false,
			0, 0, time.Second*10,
		),
	}
	t.Router()
	return t
}

func (t *TestService) Router() {
	handler.RegisterRPCResp(t.svc.GetHandler(), "测试", t.Trace)
	handler.RegisterEvent(t.svc.GetHandler(), "测试1", t.TraceString)
	handler.RegisterEvent(t.svc.GetHandler(), "测试2", t.Error)
}

func (t *TestService) Start() {
	t.svc.Start(
		func(a any) {

		},
	)
}

func (t *TestService) Stop() {
	t.svc.Stop()
}

func (t *TestService) Trace(c *ctx.Int64TraceCtx, req *basepb.IntTrace, resp *basepb.IntTrace) *berror.ErrMsg {
	t.svc.UserSubscribeOne(
		&natsclient.ServerIntUserSubject{
			ServerType: "test",
			ServerId:   0,
			RoleId:     req.RoleId,
		},
	)
	atomic.AddInt64(&count, 1)
	atomic.AddInt64(&subCount, 1)
	return nil
}

func (t *TestService) TraceString(c *ctx.Int64TraceCtx, req *basepb.StringTrace) *berror.ErrMsg {
	atomic.AddInt64(&count, 1)
	return nil
}

func (t *TestService) Error(c *ctx.Int64TraceCtx, req *basepb.ErrorMessage) *berror.ErrMsg {
	t.svc.UserUnsubscribe(
		&natsclient.ServerIntUserSubject{
			ServerType: "test",
			ServerId:   0,
			RoleId:     c.TD.RoleId,
		},
	)
	atomic.AddInt64(&count, 1)
	atomic.AddInt64(&subCount, -1)
	return nil
}

func (t *TestService) ReqTrace(roleId int64) {
	c := objectpool.Get[ctx.Int64TraceCtx]()
	defer objectpool.Put[ctx.Int64TraceCtx](c)
	c.TD.RoleId = roleId
	rpc := natsclient.NewClusterRequest[basepb.IntTrace, basepb.IntTrace]()
	rpc.Req.RoleId = roleId
	rpc.Req.FromServerId = 2
	rpc.Req.FromServerType = "3"
	rpc.Req.TraceId = "4"
	// rpc := natsclient.ClusterRequest[basepb.IntTrace, *basepb.IntTrace]{}
	err := rpc.RequestToServer(
		t.svc.GetNatsCluster(), c, baseenv.GetConfig().ServerId,
	)
	if err != nil {
		logger.Log.Panic().Err(err)
	}

	c = objectpool.Get[ctx.Int64TraceCtx]()
	defer objectpool.Put[ctx.Int64TraceCtx](c)
	rpc1 := natsclient.NewClusterPublish[basepb.StringTrace]()

	rpc1.Pub.RoleId = "x"
	rpc1.Pub.FromServerId = 2
	rpc1.Pub.FromServerType = "3"
	rpc1.Pub.TraceId = "4"

	err = rpc1.PublishToServer(
		t.svc.GetNatsCluster(), c, baseenv.GetConfig().ServerId,
	)
	if err != nil {
		logger.Log.Panic().Err(err)
	}
}
func (t *TestService) ReqUser(roleId int64) {
	c := objectpool.Get[ctx.Int64TraceCtx]()
	defer objectpool.Put[ctx.Int64TraceCtx](c)
	c.TD.RoleId = roleId
	us := &natsclient.ServerIntUserSubject{
		ServerType: "test",
		ServerId:   0,
		RoleId:     roleId,
	}
	req := natsclient.NewClusterPublishServerUser[natsclient.ServerIntUserSubject, basepb.ErrorMessage]()
	err := req.Publish(
		t.svc.GetUserNatsCluster(), us, c,
	)

	if err != nil {
		logger.Log.Panic().Err(err)
	}
}

var count int64
var subCount int64
var userCount int64

func main() {
	go func() {
		http.ListenAndServe(":9090", nil)
	}()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	svc := NewTestService()
	svc.Start()
	for i := 0; i < 1000; i++ {
		go func() {
			for {
				roleId := atomic.AddInt64(&userCount, 1)
				svc.ReqTrace(roleId)
				svc.ReqUser(roleId)
			}
		}()
	}

	oldCount := int64(0)
	for {
		time.Sleep(time.Second)
		newCount := atomic.LoadInt64(&count)
		fmt.Println(newCount-oldCount, subCount)
		oldCount = newCount
	}
	svc.Stop()
}
