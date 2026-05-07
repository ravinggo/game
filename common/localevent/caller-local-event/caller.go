package callerlocalevent

import (
	"math"
	"reflect"
	"runtime"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/logger"
)

const mark = math.MaxUint16 - 1

var (
	le localEvent
)

// eventHandler holds all metadata for a single registered local-event handler,
// including the strongly-typed handler function f, the type-erased adapter fa
// used by the publish path, a human-readable description, and any prerequisite
// calls that must succeed before this handler runs.
// Written by Claude Code claude-opus-4-6.
type eventHandler struct {
	f         any
	fa        any // for publish
	desc      string
	prevCalls []any
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

// localEvent is the global registry of all caller-style local events. It uses a
// fixed-size slot table keyed by type index and a growable slice of handler
// lists, one per registered type.
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
func Register[CP ctx.IContextPtr[CTX], T, CTX any](desc string, f func(CP, T) *berror.ErrMsg, prevCalls ...func(CP) *berror.ErrMsg) {
	pcs := make([]any, 0, len(prevCalls))
	for _, v := range prevCalls {
		pcs = append(pcs, v)
	}
	ptr, index := objectpool.GetPtrAndIndex[T]()
	setES(
		ptr, index, eventHandler{
			f: f,
			fa: func(c CP, a any) *berror.ErrMsg {
				return f(c, a.(T))
			},
			desc:      "localevent[" + desc + "] [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
			prevCalls: pcs,
		},
	)
}

// Call sync call local event
// Written by Claude Code claude-opus-4-6.
func Call[CP ctx.IContextPtr[CTX], CTX, T any](c CP, data T) *berror.ErrMsg {
	ptr, index := objectpool.GetPtrAndIndex[T]()
	es := getES(ptr, index)
	for _, e := range es {
		if len(e.prevCalls) != 0 {
			for _, pc := range e.prevCalls {
				err := pc.(func(CP) *berror.ErrMsg)(c)
				if err != nil {
					return err
				}
			}
		}
		if err := e.f.(func(CP, T) *berror.ErrMsg)(c, data); err != nil {
			return err
		}
	}
	return nil
}

// localEventKey is the context key used to store the pending events list inside
// a BaseCtx value bag, avoiding any string-key collisions.
// Written by Claude Code claude-opus-4-6.
type localEventKey struct{}

// events is the per-request accumulator of deferred publish payloads. It is
// stored in the context and drained by a middleware after the handler returns.
// Written by Claude Code claude-opus-4-6.
type events[CP ctx.IContextPtr[CTX], CTX any] struct {
	es []any
}

// add appends a single event payload to the pending list.
// Written by Claude Code claude-opus-4-6.
func (es *events[CP, CTX]) add(a any) {
	es.es = append(es.es, a)
}

// call dispatches all accumulated event payloads by looking up their registered
// handlers and invoking them in order. It drains the slice before iterating so
// that handlers which publish new events are included in a future flush. Returns
// the first error encountered, or nil if all handlers succeed.
// Written by Claude Code claude-opus-4-6.
func (es *events[CP, CTX]) call(c CP) *berror.ErrMsg {
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
			if err := ef.fa.(func(CP, any) *berror.ErrMsg)(c, e); err != nil {
				return err
			}
		}
	}
	return nil
}
