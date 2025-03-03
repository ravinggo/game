package handler

import (
	"fmt"
	"time"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
)

func LoggerAndRecover[TraceData any, TP ctx.TracePtr[TraceData]](next HandleFunc[TraceData, TP]) HandleFunc[TraceData, TP] {
	return func(c *ctx.BaseCtx[TraceData, TP]) (err *berror.ErrMsg) {
		msgName := string(define.ProtoMessageName(c.Req))
		start := time.Now()
		defer func() {
			if e := recover(); e != nil {
				err = berror.NewPanicStr(fmt.Sprintf("%v", e))
			}
			if err == nil {
				e := c.Info().Dur("duration", time.Since(start))
				if len(c.Resp) > 0 {
					for i := range c.Resp {
						e = e.Str("resp", string(define.ProtoMessageName(c.Resp[i]))).Any("respData", c.Resp[i])
					}
				}
				e.Msg("success")
			} else {
				c.Error().Err(err).Dur("duration", time.Since(start)).Msg("error")
			}
		}()

		c.Info().Str("req", msgName).Any("reqData", c.Req).Msg("start")
		err = next(c)
		return
	}
}

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
