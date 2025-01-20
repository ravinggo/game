package handler

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
)

func LoggerAndRecover[CTX ctx.IContextPtr[T], T any](next HandleFunc[CTX, T]) HandleFunc[CTX, T] {
	return func(c CTX) (err *berror.ErrMsg) {
		baseCTX := c.MustBaseContext()
		msgName := string(proto.MessageName(baseCTX.Req))
		start := time.Now()
		defer func() {
			if e := recover(); e != nil {
				err = berror.NewPanicStr(fmt.Sprintf("%v", e))
			}
			if err == nil {
				e := baseCTX.TraceLog.Info().Dur("duration", time.Since(start))
				if len(baseCTX.Resp) > 0 {
					for i := range baseCTX.Resp {
						e = e.Str("resp", string(proto.MessageName(baseCTX.Resp[i]))).Any("respData", baseCTX.Resp[i])
					}
				}
				e.Msg("success")
			} else {
				baseCTX.TraceLog.Err(err).Dur("duration", time.Since(start)).Msg("end")
			}
		}()

		baseCTX.TraceLog.Info().Str("req", msgName).Any("reqData", baseCTX.Req).Msg("start")
		err = next(c)
		return
	}
}

func Recover[CTX ctx.IContextPtr[T], T any](next HandleFunc[CTX, T]) HandleFunc[CTX, T] {
	return func(c CTX) (err *berror.ErrMsg) {
		defer func() {
			if e := recover(); e != nil {
				err = berror.NewPanicStr(fmt.Sprintf("%v", e))
				c.MustBaseContext().TraceLog.Err(err).Msg("panic")
			}
		}()
		err = next(c)
		return
	}
}
