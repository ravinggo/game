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

type localEventKey struct{}
type events[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	es []any
}

func (es *events[TraceData, TP]) add(a any) {
	es.es = append(es.es, a)
}

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
