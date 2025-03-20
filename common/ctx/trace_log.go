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
	return c.doTrace(logger.LogSkip3.Trace())
}

func (c *BaseCtx[TraceData, TP]) Debug() *logger.Event {
	return c.doTrace(logger.LogSkip3.Debug())
}

func (c *BaseCtx[TraceData, TP]) Info() *logger.Event {
	return c.doTrace(logger.LogSkip3.Info())
}

func (c *BaseCtx[TraceData, TP]) Warn() *logger.Event {
	return c.doTrace(logger.LogSkip3.Warn())
}

func (c *BaseCtx[TraceData, TP]) Error() *logger.Event {
	return c.doTrace(logger.LogSkip3.Error())
}

func (c *BaseCtx[TraceData, TP]) Fatal() *logger.Event {
	return c.doTrace(logger.LogSkip3.Fatal())
}

func (c *BaseCtx[TraceData, TP]) Panic() *logger.Event {
	return c.doTrace(logger.LogSkip3.Panic())
}

func (c *BaseCtx[TraceData, TP]) NoLevel() *logger.Event {
	return c.doTrace(logger.LogSkip3.Log())
}

func (c *BaseCtx[TraceData, TP]) Disabled() *logger.Event {
	return nil
}

func (c *BaseCtx[TraceData, TP]) WithLevel(level logger.Level) *logger.Event {
	return c.doTrace(logger.LogSkip3.WithLevel(level))
}
