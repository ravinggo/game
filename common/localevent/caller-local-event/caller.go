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

type eventHandler struct {
	f    any
	fa   any // for publish
	desc string
}

func (e eventHandler) String() string {
	return e.desc
}

type slot struct {
	ptr uintptr
	i   int
}

type localEvent struct {
	slots  [math.MaxUint16][]slot
	events [][]eventHandler
}

func getES(ptr uintptr, index int) []eventHandler {
	ess := le.slots[index]
	for _, es := range ess {
		if ptr == es.ptr {
			return le.events[es.i]
		}
	}
	return nil
}

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
func Logger() {
	for _, es := range le.events {
		for _, e := range es {
			logger.Log.Info().Str("event", e.String()).Msg("register localevent")
		}
	}
}

// Register a local event
func Register[CP ctx.IContextPtr[CTX], T, CTX any](desc string, f func(CP, T) *berror.ErrMsg) {
	ptr, index := objectpool.GetPtrAndIndex[T]()
	setES(
		ptr, index, eventHandler{
			f: f,
			fa: func(c CP, a any) *berror.ErrMsg {
				return f(c, a.(T))
			},
			desc: "localevent[" + desc + "] [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		},
	)
}

// Call sync call local event
func Call[CP ctx.IContextPtr[CTX], CTX, T any](c CP, data T) *berror.ErrMsg {
	ptr, index := objectpool.GetPtrAndIndex[T]()
	es := getES(ptr, index)
	for _, e := range es {
		if err := e.f.(func(CP, T) *berror.ErrMsg)(c, data); err != nil {
			return err
		}
	}
	return nil
}

type localEventKey struct{}
type events[CP ctx.IContextPtr[CTX], CTX any] struct {
	es []any
}

func (es *events[CP, CTX]) add(a any) {
	es.es = append(es.es, a)
}

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
