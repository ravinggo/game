package handler

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
)

func LoggerWithFilter[TraceData any, TP ctx.TracePtr[TraceData]](msgS ...proto.Message) Middleware[TraceData, TP] {
	filter := make(map[protoreflect.FullName]struct{}, len(msgS))
	for _, msg := range msgS {
		filter[define.ProtoMessageName(msg)] = struct{}{}
	}
	return func(next HandleFunc[TraceData, TP]) HandleFunc[TraceData, TP] {
		return func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
			if len(filter) != 0 && c.Req != nil {
				if _, ok := filter[define.ProtoMessageName(c.Req)]; ok {
					return next(c)
				}
			}
			return Logger(next)(c)
		}
	}
}

// Logger logs the inbound request, calls the next handler, then logs the outcome with duration.
// On success it attaches all response payloads to the log line for auditability.
func Logger[TraceData any, TP ctx.TracePtr[TraceData]](next HandleFunc[TraceData, TP]) HandleFunc[TraceData, TP] {
	return func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
		start := time.Now()
		c.Info().Any("reqData", c.Req).Msg("start")
		err := next(c)
		if err == nil {
			e := c.Info().Dur("duration", time.Since(start))
			if c.Resp != nil {
				e = e.Any("respData", c.Resp)
			}
			for _, r := range c.OtherResp {
				e = e.Any("respData", r)
			}
			e.Msg("success")
		} else {
			c.Error().Err(err).Dur("duration", time.Since(start)).Msg("error")
		}
		return err
	}
}

// Recover is a lightweight panic-recovery middleware. Unlike LoggerAndRecover it does not emit
// success logs or measure duration; it solely catches panics, converts them to an ErrMsg, and
// logs the stack at error level. Use it in hot paths where the full logging overhead is unwanted
// but defensive recovery is still required.
// Written by Claude Code claude-opus-4-6.
func Recover[TraceData any, TP ctx.TracePtr[TraceData]](next HandleFunc[TraceData, TP]) HandleFunc[TraceData, TP] {
	return func(c *ctx.BaseCtx[TraceData, TP]) (err *berror.ErrMsg) {
		defer func() {
			if e := recover(); e != nil {
				err = berror.NewPanicStr(fmt.Sprintf("%v", e))
				c.Error().Err(err).Msg("panic")
			}
		}()
		err = next(c)
		return
	}
}
