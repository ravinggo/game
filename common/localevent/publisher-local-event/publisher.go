package publisherlocalevent

import (
	"math"
	"reflect"
	"runtime"
	"sync"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
)

const mark = math.MaxUint16 - 1

var (
	le     localEvent
	esPool *sync.Pool
)

// eventHandler holds all metadata for a single registered publisher local-event
// handler, including the strongly-typed handler function f, the type-erased
// adapter fa used by the publish path, and a human-readable description.
// Written by Claude Code claude-opus-4-6.
type eventHandler struct {
	f    any
	fa   any // for publish
	desc string
}

// String returns a human-readable description of the eventHandler, useful for
// logging and diagnostics.
// Written by Claude Code claude-opus-4-6.
func (e eventHandler) String() string {
	return e.desc
}

// slot maps a type pointer to its position in the events slice of localEvent,
// allowing O(n) lookup by type identity.
// Written by Claude Code claude-opus-4-6.
type slot struct {
	ptr uintptr
	i   int
}

// localEvent is the global registry of all publisher-style local events. It
// uses a fixed-size slot table keyed by type index and a growable slice of
// handler lists, one per registered type.
// Written by Claude Code claude-opus-4-6.
type localEvent struct {
	slots  [math.MaxUint16][]slot
	events [][]eventHandler
}

// getES looks up and returns the handler list for the type identified by (ptr,
// index). Returns nil when no handlers have been registered for that type.
// Written by Claude Code claude-opus-4-6.
func getES(ptr uintptr, index int) []eventHandler {
	ess := le.slots[index]
	for _, es := range ess {
		if ptr == es.ptr {
			return le.events[es.i]
		}
	}
	return nil
}

// setES appends handler e to the handler list for the type identified by (ptr,
// index). If no list exists yet it is created and linked into the registry.
// Written by Claude Code claude-opus-4-6.
func setES(ptr uintptr, index int, e eventHandler) {
	ess := le.slots[index]
	var es []eventHandler
	var i int
	found := false
	for _, v := range ess {
		if ptr == v.ptr {
			i = v.i
			es = le.events[i]
			found = true
			break
		}
	}
	if !found {
		i = len(le.events)
		le.slots[index] = append(le.slots[index], slot{ptr: ptr, i: i})
		es = make([]eventHandler, 0, 4)
		es = append(es, e)
		le.events = append(le.events, es)
		return
	}
	es = append(es, e)
	le.events[i] = es
}

// Logger print all local event
// Written by Claude Code claude-opus-4-6.
func Logger() {
	for _, es := range le.events {
		for _, e := range es {
			logger.Log.Info().Str("event", e.String()).Msg("register localevent")
		}
	}
}

// Register a local event
// Written by Claude Code claude-opus-4-6.
func Register[TraceData, T any, TP ctx.TracePtr[TraceData]](desc string, f func(*ctx.BaseCtx[TraceData, TP], T) *berror.ErrMsg) {
	if esPool == nil {
		esPool = objectpool.GetTypePool[events[TraceData, TP]]()
	}
	ptr, index := objectpool.GetPtrAndIndex[T]()
	setES(
		ptr, index, eventHandler{
			f: f,
			fa: func(c *ctx.BaseCtx[TraceData, TP], a any) *berror.ErrMsg {
				return f(c, a.(T))
			},
			desc: "localevent[" + desc + "] [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		},
	)
}

// Call sync call local event
// Written by Claude Code claude-opus-4-6.
func Call[TraceData, T any, TP ctx.TracePtr[TraceData]](c *ctx.BaseCtx[TraceData, TP], data T) *berror.ErrMsg {
	ptr, index := objectpool.GetPtrAndIndex[T]()
	es := getES(ptr, index)
	for _, e := range es {
		if err := e.f.(func(*ctx.BaseCtx[TraceData, TP], T) *berror.ErrMsg)(c, data); err != nil {
			return err
		}
	}
	return nil
}

// localEventKey is the context key used to store the pending events list inside
// a BaseCtx value bag, avoiding any string-key collisions.
// Written by Claude Code claude-opus-4-6.
type localEventKey struct{}

// events is the per-request accumulator of deferred publish payloads. Instances
// are object-pooled via esPool and stored in the request context. They are
// drained and returned to the pool by MiddleLocalEvent after each handler.
// Written by Claude Code claude-opus-4-6.
type events[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	es []any
}

// add appends a single event payload to the pending list.
// Written by Claude Code claude-opus-4-6.
func (es *events[TraceData, TP]) add(a any) {
	es.es = append(es.es, a)
}

// call dispatches all accumulated event payloads by looking up their registered
// handlers and invoking them in order. It drains the slice before iterating so
// that handlers which publish new events are included in a future flush. Returns
// the first error encountered, or nil if all handlers succeed.
// Written by Claude Code claude-opus-4-6.
func (es *events[TraceData, TP]) call(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
	cs := es.es
	es.es = es.es[:0]
	if len(cs) == 0 {
		return nil
	}
	for _, e := range cs {
		ptr, index := objectpool.GetPtrAnyAndIndex(e)
		es := getES(ptr, index)
		if len(es) == 0 {
			logger.Log.Warn().Str("eventName", reflect.TypeOf(e).String()).Msg("not found localevent")
			continue
		}
		for _, ef := range es {
			if err := ef.fa.(func(*ctx.BaseCtx[TraceData, TP], any) *berror.ErrMsg)(c, e); err != nil {
				return err
			}
		}
	}
	return nil
}

// Publish local event, async call by MiddleLocalEvent
// Written by Claude Code claude-opus-4-6.
func Publish[TraceData any, TP ctx.TracePtr[TraceData]](c *ctx.BaseCtx[TraceData, TP], data any) {
	es, ok := c.Value(localEventKey{}).(*events[TraceData, TP])
	if !ok {
		es = esPool.Get().(*events[TraceData, TP])
		if cap(es.es) == 0 {
			es.es = make([]any, 0, 8)
		}
		c.SetValue(localEventKey{}, es)
	}
	es.es = append(es.es, data)
}

// MiddleLocalEvent local event middleware,call Publish local event
// Written by Claude Code claude-opus-4-6.
func MiddleLocalEvent[TraceData any, TP ctx.TracePtr[TraceData]](
	next handler.HandleFunc[TraceData, TP],
) handler.HandleFunc[TraceData, TP] {
	return func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
		err := next(c)
		if err != nil {
			return err
		}
		v := c.Value(localEventKey{})
		if v == nil {
			return nil
		}
		es := v.(*events[TraceData, TP])
		err = es.call(c)
		esPool.Put(es)
		return err
	}
}
