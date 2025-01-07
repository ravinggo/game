package service

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
	"github.com/ravinggo/game/common/objectpool"
)

type TestService struct {
	svc *BaseService[*ctx.Int64TraceCtx, ctx.Int64TraceCtx]
}

func NewTestService() *TestService {
	t := &TestService{
		svc: NewBaseService[*ctx.Int64TraceCtx](
			[]string{"nats://127.0.0.1:4224"},
			false,
			0, 0, time.Second*10,
		),
	}
	t.Router()
	return t
}

func (t *TestService) Router() {
	handler.RegisterRPCRespSingle(t.svc.GetHandler(), "测试", t.Trace)
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
	resp.RoleId = req.RoleId
	return nil
}

func (t *TestService) ReqTrace() {
	resp := &basepb.IntTrace{}
	c := objectpool.Get[ctx.Int64TraceCtx]()
	defer objectpool.Put[ctx.Int64TraceCtx](c)
	c.TD.TraceId = "xxx"
	c.TD.RoleId = 20
	err := t.svc.GetNatsCluster().RequestToServer(
		c, baseenv.GetConfig().ServerId, &basepb.IntTrace{
			RoleId:         1,
			FromServerId:   2,
			FromServerType: "3",
			TraceId:        "4",
		}, resp,
	)
	if err != nil {
		logger.Log.Panic().Err(err)
	}

	req := natsclient.ClusterRequest[basepb.IntTrace]{}
	err = req.RequestToServer(
		t.svc.GetNatsCluster(), c, baseenv.GetConfig().ServerId, &basepb.IntTrace{
			RoleId:         2,
			FromServerId:   2,
			FromServerType: "3",
			TraceId:        "4",
		},
	)
	if err != nil {
		logger.Log.Panic().Err(err)
	}
	atomic.AddInt64(&count, 2)
}

var count int64

func TestBaseService(t *testing.T) {
	go func() {
		http.ListenAndServe(":9090", nil)
	}()
	svc := NewTestService()
	svc.Start()
	for i := 0; i < 2000; i++ {
		go func() {
			for {
				svc.ReqTrace()
			}
		}()

	}
	oldCount := int64(0)
	for {
		time.Sleep(time.Second)
		newCount := atomic.LoadInt64(&count)
		fmt.Println(newCount - oldCount)
		oldCount = newCount
	}
	svc.Stop()
}
