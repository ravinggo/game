package handler

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/objectpool"
)

type (
	MiddleWare[CTX ctx.IContext]   func(HandleFunc[CTX]) HandleFunc[CTX]
	HandleFunc[CTX ctx.IContext]   func(ctx CTX) *berror.ErrMsg
)

type Elem[CTX ctx.IContext] struct {
	h        HandleFunc[CTX]
	desc     string
	midS     []MiddleWare[CTX]
	isRPC    bool
	msgName  protoreflect.FullName
	funcType string
	isForce  bool
	isSingle bool
	reqPool  *sync.Pool
	respPool *sync.Pool
}

func (this_ *Elem[CTX]) IsForce() bool {
	return this_.isForce
}

func (this_ *Elem[CTX]) MsgName() string {
	return string(this_.msgName)
}

func (this_ *Elem[CTX]) IsSingle() bool {
	return this_.isSingle
}

func (this_ *Elem[CTX]) IsRPC() bool {
	return this_.isRPC
}

func (this_ *Elem[CTX]) ReqPool() *sync.Pool {
	return this_.reqPool
}

func (this_ *Elem[CTX]) RespPool() *sync.Pool {
	return this_.respPool
}

func (this_ *Elem[CTX]) String() string {
	if this_.isRPC {
		return fmt.Sprintf("[%s] func: %s", this_.desc, this_.funcType)
	}
	return fmt.Sprintf("[%s] rpc: %s", this_.desc, this_.funcType)
}

func (this_ *Elem[CTX]) Call(c CTX) *berror.ErrMsg {
	hf := this_.call(0, c)
	return hf(c)
}

func (this_ *Elem[CTX]) call(i int, c CTX) HandleFunc[CTX] {
	if i == len(this_.midS) {
		return this_.h
	}
	hf := this_.call(i+1, c)
	return this_.midS[i](hf)
}

type Handler[CTX ctx.IContext] struct {
	handle         map[protoreflect.FullName]*Elem[CTX]
	subjMap        map[string]struct{}
	baseMiddleware []MiddleWare[CTX]
	broadcastSubj  map[string]struct{}
}

// NewHandler create Router Handler
func NewHandler[T ctx.IContext](middlewares ...MiddleWare[T]) *Handler[T] {
	h := &Handler[T]{
		handle:        map[protoreflect.FullName]*Elem[T]{},
		subjMap:       map[string]struct{}{},
		broadcastSubj: map[string]struct{}{},
	}
	h.baseMiddleware = append(h.baseMiddleware, middlewares...)
	return h
}

// Group create a new Handler with same base middlewares
func (h *Handler[CTX]) Group(middlewares ...MiddleWare[CTX]) *Handler[CTX] {
	newH := &Handler[CTX]{
		handle:  h.handle,
		subjMap: h.subjMap,
	}
	newH.baseMiddleware = append(newH.baseMiddleware, h.baseMiddleware...)
	newH.baseMiddleware = append(newH.baseMiddleware, middlewares...)
	return newH
}

// GetHandler get Elem by msgName or nil
func (h *Handler[CTX]) GetHandler(msgName string) (*Elem[CTX], bool) {
	e, ok := h.handle[protoreflect.FullName(msgName)]
	return e, ok
}

// GetQueueSubjInfo get all queue subj topic prefix
func (h *Handler[CTX]) GetQueueSubjInfo() map[string]struct{} {
	return h.subjMap
}

// GetBroadcastSubjInfo get all broadcast subj topic prefix
func (h *Handler[CTX]) GetBroadcastSubjInfo() map[string]struct{} {
	return h.broadcastSubj
}

// RegisterRPC RESP is out parameter and Call in many goroutines,just one sub will receive the event
func RegisterRPC[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[T],
) {
	registerRPC(h, desc, f, false, false, middlewares...)
}

// RegisterRPCSingle RESP is out parameter and Call on eventloop,just one sub will receive the event
func RegisterRPCSingle[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[T],
) {
	registerRPC(h, desc, f, false, true, middlewares...)
}

// RegisterRPCForce f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterRPCForce[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[T],
) {
	registerRPC(h, desc, f, true, false, middlewares...)
}

// RegisterRPCForceSingle f is force handle when Server is busy on eventloop，just one sub will receive the event
func RegisterRPCForceSingle[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[T],
) {
	registerRPC(h, desc, f, true, true, middlewares...)
}

func getSubjPrefix(msgName string) string {
	return msgName[:strings.LastIndexByte(msgName, '.')+1]
}

func registerRPC[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ) (RESP, *berror.ErrMsg), isForceHandle, isSingle bool, middlewares ...MiddleWare[T],
) {
	var req REQ
	msgName := proto.MessageName(req)
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if _, ok := h.broadcastSubj[subj]; ok {
			panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
		}
		h.subjMap[subj] = struct{}{}
	}

	midS := make([]MiddleWare[T], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[T]{
		h: func(c T) *berror.ErrMsg {
			bc := c.MustBaseContext()
			ret, err := f(c, bc.Req.(REQ))
			if err != nil {
				return err
			}
			bc.Resp = append(bc.Resp, ret)
			return nil
		},
		desc:     desc,
		midS:     midS,
		isRPC:    true,
		msgName:  msgName,
		isForce:  isForceHandle,
		isSingle: isSingle,
		funcType: " [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		reqPool:  objectpool.GetTypeElemPool[REQ](),
		respPool: objectpool.GetTypeElemPool[RESP](),
	}
}

// RegisterRPCResp RESP is in parameter and Call in many goroutines，just one sub will receive the event
func RegisterRPCResp[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerRPCResp(h, desc, f, false, false, middlewares...)
}

// RegisterRPCRespSingle RESP is in parameter and Call in eventloop，just one sub will receive the event
func RegisterRPCRespSingle[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerRPCResp(h, desc, f, false, true, middlewares...)
}

// RegisterRPCRespForce f is force handle when Server is busy many goroutines
// It is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterRPCRespForce[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerRPCResp(h, desc, f, true, false, middlewares...)
}

// RegisterRPCRespForceSingle f is force handle when Server is busy on eventloop，just one sub will receive the event
func RegisterRPCRespForceSingle[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerRPCResp(h, desc, f, true, true, middlewares...)
}

func registerRPCResp[T ctx.IContext, REQ, RESP proto.Message](
	h *Handler[T], desc string, f func(T, REQ, RESP) *berror.ErrMsg, isForceHandle, isSingle bool, middlewares ...MiddleWare[T],
) {
	var req REQ
	msgName := proto.MessageName(req)
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if _, ok := h.broadcastSubj[subj]; ok {
			panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
		}
		h.subjMap[subj] = struct{}{}
	}

	midS := make([]MiddleWare[T], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	respPool := objectpool.GetTypeElemPool[RESP]()
	h.handle[msgName] = &Elem[T]{
		h: func(c T) *berror.ErrMsg {
			bc := c.MustBaseContext()
			resp := respPool.Get().(RESP)
			err := f(c, bc.Req.(REQ), resp)
			if err != nil {
				return err
			}
			bc.Resp = append(bc.Resp, resp)
			return nil
		},
		desc:     desc,
		midS:     midS,
		isRPC:    true,
		msgName:  msgName,
		isForce:  isForceHandle,
		isSingle: isSingle,
		funcType: " [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		reqPool:  objectpool.GetTypeElemPool[REQ](),
		respPool: respPool,
	}
}

// RegisterEvent Call in many goroutines, just one sub will receive the event
func RegisterEvent[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, false, false, false, middlewares...)
}

// RegisterEventSingle Call in eventloop, just one sub will receive the event
func RegisterEventSingle[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, false, true, false, middlewares...)
}

// RegisterEventForce f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterEventForce[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, true, false, false, middlewares...)
}

// RegisterEventForceSingle f is force handle when Server is busy on eventloop
// just one sub will receive the event
func RegisterEventForceSingle[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, true, true, false, middlewares...)
}

func registerEvent[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, isForceHandle, isSingle, isBroadcast bool, middlewares ...MiddleWare[T],
) {
	var req REQ
	msgName := proto.MessageName(req)
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if isBroadcast {
			if _, ok := h.subjMap[subj]; ok {
				panic(fmt.Sprintf("subj %s already registered as normal handler,not is broadcast message[%s]", msgName, v.String()))
			}
			h.broadcastSubj[subj] = struct{}{}
		} else {
			if _, ok := h.broadcastSubj[subj]; ok {
				panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
			}
			h.subjMap[subj] = struct{}{}
		}
	}

	midS := make([]MiddleWare[T], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[T]{
		h: func(c T) *berror.ErrMsg {
			bc := c.MustBaseContext()
			return f(c, bc.Req.(REQ))
		},
		desc:     desc,
		midS:     midS,
		isRPC:    false,
		msgName:  msgName,
		isForce:  isForceHandle,
		isSingle: isSingle,
		funcType: " [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		reqPool:  objectpool.GetTypeElemPool[REQ](),
	}
}

// RegisterEventBroadcast Call in many goroutines,and all subscribers will receive
func RegisterEventBroadcast[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, false, false, true, middlewares...)
}

// RegisterEventSingleBroadcast Call in eventloop ,and all subscribers will receive
func RegisterEventSingleBroadcast[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, false, true, true, middlewares...)
}

// RegisterEventForceBroadcast f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// all subscribers will receive
func RegisterEventForceBroadcast[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, true, false, true, middlewares...)
}

// RegisterEventForceSingleBroadcast f is force handle when Server is busy on eventloop
// all subscribers will receive
func RegisterEventForceSingleBroadcast[T ctx.IContext, REQ proto.Message](
	h *Handler[T], desc string, f func(T, REQ) *berror.ErrMsg, middlewares ...MiddleWare[T],
) {
	registerEvent(h, desc, f, true, true, true, middlewares...)
}
