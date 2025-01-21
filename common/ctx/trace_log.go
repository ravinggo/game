package ctx

import (
	"github.com/ravinggo/game/common/logger"
)

func (c *BaseCtx[TraceData, TP]) doTrace(e *logger.Event) *logger.Event {
	trace := c.GetTrace()
	if trace != nil {
		e = trace.TraceLogField(e)
	}
	return e
}

func (c *BaseCtx[TraceData, TP]) Trace() *logger.Event {
	return c.doTrace(logger.Log.Trace())
}

func (c *BaseCtx[TraceData, TP]) Debug() *logger.Event {
	return c.doTrace(logger.Log.Debug())
}

func (c *BaseCtx[TraceData, TP]) Info() *logger.Event {
	return c.doTrace(logger.Log.Info())
}

func (c *BaseCtx[TraceData, TP]) Warn() *logger.Event {
	return c.doTrace(logger.Log.Warn())
}

func (c *BaseCtx[TraceData, TP]) Error() *logger.Event {
	return c.doTrace(logger.Log.Error())
}

func (c *BaseCtx[TraceData, TP]) Fatal() *logger.Event {
	return c.doTrace(logger.Log.Fatal())
}

func (c *BaseCtx[TraceData, TP]) Panic() *logger.Event {
	return c.doTrace(logger.Log.Panic())
}

func (c *BaseCtx[TraceData, TP]) NoLevel() *logger.Event {
	return c.doTrace(logger.Log.Log())
}

func (c *BaseCtx[TraceData, TP]) Disabled() *logger.Event {
	return nil
}

func (c *BaseCtx[TraceData, TP]) WithLevel(level logger.Level) *logger.Event {
	return c.doTrace(logger.Log.WithLevel(level))
}
