package ctx

import (
	"github.com/ravinggo/game/common/logger"
)

var log logger.Logger

func init() {
	log = logger.Log.With().CallerWithSkipFrameCount(3).Logger()
}

func (c *BaseCtx[TraceData, TP]) doTrace(e *logger.Event) *logger.Event {
	trace := c.GetTrace()
	if trace != nil {
		e = trace.TraceLogField(e)
	}
	return e
}

func (c *BaseCtx[TraceData, TP]) Trace() *logger.Event {
	return c.doTrace(log.Trace())
}

func (c *BaseCtx[TraceData, TP]) Debug() *logger.Event {
	return c.doTrace(log.Debug())
}

func (c *BaseCtx[TraceData, TP]) Info() *logger.Event {
	return c.doTrace(log.Info())
}

func (c *BaseCtx[TraceData, TP]) Warn() *logger.Event {
	return c.doTrace(log.Warn())
}

func (c *BaseCtx[TraceData, TP]) Error() *logger.Event {
	return c.doTrace(log.Error())
}

func (c *BaseCtx[TraceData, TP]) Fatal() *logger.Event {
	return c.doTrace(log.Fatal())
}

func (c *BaseCtx[TraceData, TP]) Panic() *logger.Event {
	return c.doTrace(log.Panic())
}

func (c *BaseCtx[TraceData, TP]) NoLevel() *logger.Event {
	return c.doTrace(log.Log())
}

func (c *BaseCtx[TraceData, TP]) Disabled() *logger.Event {
	return nil
}

func (c *BaseCtx[TraceData, TP]) WithLevel(level logger.Level) *logger.Event {
	return c.doTrace(log.WithLevel(level))
}
