package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/ravinggo/objectpool"
	"github.com/ravinggo/zerolog"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/localevent"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/service"
)

type TestService struct {
	svc *service.ServerUserService[natsclient.ServerIntUserSubject, ctx.IntTrace, *ctx.IntTrace, *natsclient.ServerIntUserSubject]
	nc  *natsclient.ClusterClientServerUser[natsclient.ServerIntUserSubject, *natsclient.ServerIntUserSubject]
}

func NewTestService() *TestService {
	t := &TestService{
		svc: service.NewServerUserService[natsclient.ServerIntUserSubject, ctx.IntTrace](
			[]string{
				"nats://192.168.0.160:4224",
			},
			service.MiddlesOption(handler.LoggerAndRecover[ctx.IntTrace, *ctx.IntTrace]),
		),
	}
	t.nc = natsclient.NewClusterClientServerUser[natsclient.ServerIntUserSubject](
		[]string{
			"nats://192.168.0.160:4224",
		},
		t.svc.DealServerUserNatsMsg,
	)
	t.Router()
	return t
}

func (t *TestService) Router() {
	handler.RegisterRPCResp(t.svc.GetHandler(), "test1", t.Trace)
	handler.RegisterEvent(t.svc.GetHandler(), "test2", t.TraceString)
	handler.RegisterRPCResp(t.svc.GetHandler(), "test3", t.Error)
	localevent.Register("test localevent eventString", t.eventString)
	localevent.Register("test localevent eventInt", t.eventInt)
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
	if req.RoleId%100 == 0 {
		us := &natsclient.ServerIntUserSubject{
			ServerType: "test",
			ServerId:   0,
			RoleId:     req.RoleId,
		}
		t.svc.UserSubscribeOneWaitSuccess(
			us,
		)
		atomic.AddInt64(&subCount, 1)
	}
	atomic.AddInt64(&count, 1)
	resp.RoleId = req.RoleId
	return localevent.Call(c, req)
}

func (t *TestService) eventString(c *ctx.Int64TraceCtx, req *basepb.StringTrace) *berror.ErrMsg {
	return nil
}

func (t *TestService) eventInt(c *ctx.Int64TraceCtx, req *basepb.IntTrace) *berror.ErrMsg {
	return nil
}

func (t *TestService) TraceString(c *ctx.Int64TraceCtx, req *basepb.StringTrace) *berror.ErrMsg {
	err := localevent.Call(c, req)
	if err != nil {
		return err
	}
	atomic.AddInt64(&count, 1)
	return nil
}

func (t *TestService) Error(c *ctx.Int64TraceCtx, req *basepb.ErrorMessage, resp *basepb.IntTrace) *berror.ErrMsg {
	if c.TD.RoleId != 0 {
		atomic.AddInt64(&subCount, -1)
		t.svc.UserUnsubscribe(
			&natsclient.ServerIntUserSubject{
				ServerType: "test",
				ServerId:   0,
				RoleId:     c.TD.RoleId,
			},
		)
	}
	atomic.AddInt64(&count, 1)
	return nil
}

func (t *TestService) ReqTrace(roleId int64) {
	c := objectpool.Get[ctx.Int64TraceCtx]()
	defer objectpool.Put[ctx.Int64TraceCtx](c)
	c.TD.RoleId = roleId
	rpc := natsclient.NewClusterRequest[basepb.IntTrace, basepb.IntTrace]()
	defer rpc.Free()
	rpc.Req.RoleId = roleId
	rpc.Req.FromServerId = 2
	rpc.Req.FromServerType = "3"
	rpc.Req.TraceId = "4"
	// rpc := natsclient.ClusterRequest[basepb.IntTrace, *basepb.IntTrace]{}
	err := rpc.RequestToServer(
		t.nc.ClusterClient, c, baseenv.GetConfig().ServerId,
	)
	if err != nil {
		logger.Log.Error().Err(err).Msg("ReqTrace IntTrace")
	}

	c = objectpool.Get[ctx.Int64TraceCtx]()
	defer objectpool.Put[ctx.Int64TraceCtx](c)

	rpc1 := natsclient.NewClusterPublish[basepb.StringTrace]()
	defer rpc1.Free()
	rpc1.Pub.RoleId = "x"
	rpc1.Pub.FromServerId = 2
	rpc1.Pub.FromServerType = "3"
	rpc1.Pub.TraceId = "4"

	err = rpc1.PublishToServer(
		t.nc.ClusterClient, c, baseenv.GetConfig().ServerId,
	)
	if err != nil {
		logger.Log.Panic().Err(err).Msg("ReqTrace StringTrace")
	}
	if roleId%100 == 0 {

		c = objectpool.Get[ctx.Int64TraceCtx]()
		defer objectpool.Put[ctx.Int64TraceCtx](c)

		if rpc.Resp.RoleId == roleId {
			c.TD.RoleId = roleId
			c.TD.FromServerId = 0
		}

		req := natsclient.NewClusterRequestServerUser[basepb.ErrorMessage, basepb.IntTrace, natsclient.ServerIntUserSubject]()
		defer req.Free()
		req.Us.ServerType = "test"
		req.Us.RoleId = roleId
		err = req.Request(t.nc, c)

		if err != nil {
			logger.Log.Panic().Err(err).Int64("roleId", roleId).Msg("ReqUser ErrorMessage")
		} else {
			// logger.Log.Info().Int64("roleId", roleId).Msg("ReqUser ErrorMessage success")
		}
	}
}

var count int64
var subCount int64
var userCount int64 = 1

func main() {
	go func() {
		http.ListenAndServe(":9090", nil)
	}()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	svc := NewTestService()
	svc.Start()
	for i := 0; i < 1; i++ {
		go func() {
			for {
				roleId := atomic.AddInt64(&userCount, 1)
				svc.ReqTrace(roleId)
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
