package localevent

import (
	"math"
	"reflect"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/handler"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
)

const mark = math.MaxUint16 - 1

var (
	le     localEvent
	esPool = objectpool.GetTypePool[events]()
)

type eventHandler struct {
	f    any
	fa   any
	desc string
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

func calcIndex[T any]() (uintptr, int) {
	ptr := objectpool.GetPtr[T]()
	index := int((ptr >> 6) & mark)
	return ptr, index
}

func calcIndex2(a any) (uintptr, int) {
	ptr := objectpool.GetPtrAny(a)
	index := int((ptr >> 6) & mark)
	return ptr, index
}

func Register[T any](desc string, f func(ctx.IContext, T) *berror.ErrMsg) {
	ptr, index := calcIndex[T]()
	setES(
		ptr, index, eventHandler{
			f: f,
			fa: func(c ctx.IContext, a any) *berror.ErrMsg {
				return f(c, a.(T))
			},
		},
	)
}

func Call[T any](c ctx.IContext, data T) *berror.ErrMsg {
	ptr, index := calcIndex[T]()
	es := getES(ptr, index)
	for _, e := range es {
		if err := e.f.(func(ctx.IContext, T) *berror.ErrMsg)(c, data); err != nil {
			return err
		}
	}
	return nil
}

type localEventKey struct{}
type events struct {
	es []any
}

func (es *events) add(a any) {
	es.es = append(es.es, a)
}

func (es *events) call(c ctx.IContext) *berror.ErrMsg {
	cs := es.es
	es.es = es.es[:0]
	if len(cs) == 0 {
		return nil
	}
	for _, e := range cs {
		ptr, index := calcIndex2(e)
		es := getES(ptr, index)
		if len(es) == 0 {
			logger.Log.Warn().Str("eventName", reflect.TypeOf(e).String()).Msg("not found localevent")
			continue
		}
		for _, ef := range es {
			if err := ef.fa.(func(ctx.IContext, any) *berror.ErrMsg)(c, e); err != nil {
				return err
			}
		}
	}
	return nil
}

func Publish(c ctx.IContext, data any) {
	es, ok := c.Value(localEventKey{}).(*events)
	if !ok {
		es = esPool.Get().(*events)
		if cap(es.es) == 0 {
			es.es = make([]any, 0, 8)
		}
		c.MustBaseContext().SetValue(localEventKey{}, es)
	}
	es.es = append(es.es, data)
}

func MiddleLocalEvent[CTX ctx.IContextPtr[T], T any](
	next handler.HandleFunc[CTX, T],
) handler.HandleFunc[CTX, T] {
	return func(c CTX) *berror.ErrMsg {
		err := next(c)
		if err != nil {
			return err
		}
		v := c.Value(localEventKey{})
		if v == nil {
			return nil
		}
		es := v.(*events)
		err = es.call(c)
		esPool.Put(es)
		return err
	}
}
