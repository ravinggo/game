package service

import (
	"reflect"
	"time"

	baseenv "github.com/ravinggo/game/common/base-env"
	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/natsclient"
)

type handlerElemKey struct{}

type BaseService[CTX ctx.IContext] struct {
	h           *handler.Handler[CTX]
	natsCluster *natsclient.ClusterClient
	el          *eventloop.DoubleBuffQueue
}

func NewBaseService[CTX ctx.IContext](
	natsUrls []string,
	lockQueueThread bool,
	rpcTimeout time.Duration,
) *BaseService[CTX] {
	return &BaseService[CTX]{
		h:           handler.NewHandler[CTX](),
		natsCluster: natsclient.NewClusterClient(baseenv.GetConfig().ServerType, natsUrls, rpcTimeout),
		el:          eventloop.NewDoubleBuffQueue(lockQueueThread),
	}
}

func (s *BaseService[CTX]) GetHandler() *handler.Handler[CTX] {
	return s.h
}

func (s *BaseService[CTX]) GetNatsCluster() *natsclient.ClusterClient {
	return s.natsCluster
}

func (s *BaseService[CTX]) PostEventloop(e any) {
	s.el.PostEventQueue(e)
}

func (s *BaseService[CTX]) Start(f func(any)) {
	s.el.Start(
		func(e any) {
			switch c := e.(type) {
			case CTX:
				s.handleCtx(c)
			default:
				f(e)
			}
		},
	)
}

func (s *BaseService[CTX]) handleCtx(c CTX) {
	e, ok := ctx.Value[handlerElemKey, *handler.Elem[CTX]](c, handlerElemKey{})
	if ok {
		s.call(c, e)
	} else {
		logger.Log.Warn().Msgf("invalid %s,ctx not found handler elem", reflect.TypeOf(c).String())
	}
}

func (s *BaseService[CTX]) call(c CTX, e *handler.Elem[CTX]) {
	var err *berror.ErrMsg
	start := time.Now()
	baseCtx := c.MustBaseContext()
	defer func() {
		if err != nil {
			logger.Log.Warn().
				Str("req", e.MsgName()).
				Err(err).
				Dur("cost", time.Since(start)).
				Msg("handle error")
			if baseCtx.NatsMsg != nil {
				err = natsclient.NatsReplyError(baseCtx.NatsMsg, err)
				if err != nil {
					baseCtx.TraceLog.Error().Err(err).Msg("nats reply error")
				}
			}
		}

		baseCtx.TraceLog.Debug().
			Str("req", e.MsgName()).
			Dur("cost", time.Since(start)).
			Msg("handle success")
	}()
	baseCtx.TraceLog.Debug().Str("req", e.MsgName()).Msg("handle start")
	err = e.Call(c)
	if err != nil {
		return
	}
	if baseCtx.NatsMsg != nil && baseCtx.Resp != nil {
		err = natsclient.NatsMsgReply(baseCtx.NatsMsg, baseCtx.Resp...)
		if err != nil {
			return
		}
	}
}

func (s *BaseService[CTX]) Stop() {
	s.el.Stop()
	s.natsCluster.Shutdown()
}
