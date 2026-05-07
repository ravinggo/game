package ctx

import (
	"github.com/ravinggo/game/common/logger"
)

// doTrace enriches a zerolog Event with the fields from the context's Trace (e.g. traceId,
// roleId, fromServerId). It is an internal helper shared by all log-level methods.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) doTrace(e *logger.Event) *logger.Event {
	trace := c.GetTrace()
	if trace != nil {
		e = trace.TraceLogField(e)
	}
	return e
}

// Trace returns a TRACE-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Trace() *logger.Event {
	return c.doTrace(logger.LogSkip3.Trace())
}

// Debug returns a DEBUG-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Debug() *logger.Event {
	return c.doTrace(logger.LogSkip3.Debug())
}

// Info returns an INFO-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Info() *logger.Event {
	return c.doTrace(logger.LogSkip3.Info())
}

// Warn returns a WARN-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Warn() *logger.Event {
	return c.doTrace(logger.LogSkip3.Warn())
}

// Error returns an ERROR-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Error() *logger.Event {
	return c.doTrace(logger.LogSkip3.Error())
}

// Fatal returns a FATAL-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Fatal() *logger.Event {
	return c.doTrace(logger.LogSkip3.Fatal())
}

// Panic returns a PANIC-level zerolog Event annotated with the context's trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Panic() *logger.Event {
	return c.doTrace(logger.LogSkip3.Panic())
}

// NoLevel returns a zerolog Event with no severity level, annotated with trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) NoLevel() *logger.Event {
	return c.doTrace(logger.LogSkip3.Log())
}

// Disabled always returns nil, satisfying the ILogger interface for contexts where
// logging is intentionally suppressed.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Disabled() *logger.Event {
	return nil
}

// WithLevel returns a zerolog Event at the specified level, annotated with trace fields.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) WithLevel(level logger.Level) *logger.Event {
	return c.doTrace(logger.LogSkip3.WithLevel(level))
}
