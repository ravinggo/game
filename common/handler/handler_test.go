package handler

import (
	"fmt"
	"reflect"
	"runtime"
	"testing"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
)

type Server struct {
}

func (s *Server) Test1(c *ctx.Int64TraceCtx, req *basepb.IntTrace) (*basepb.StringTrace, *berror.ErrMsg) {
	return nil, nil
}

func (s *Server) Test2(c *ctx.Int64TraceCtx, req *basepb.IntTrace, resp *basepb.StringTrace) *berror.ErrMsg {
	return nil
}

func TestHandler(t *testing.T) {
	h := NewHandler[ctx.IntTrace]()
	s := &Server{}
	fn := "[" + runtime.FuncForPC(reflect.ValueOf(s.Test1).Pointer()).Name() + "] " + reflect.TypeOf(s.Test1).String()
	fmt.Println(fn)
	RegisterRPC(
		h, "test", s.Test1,
	)
	RegisterRPCResp(
		h, "test", s.Test2,
	)
}
