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
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
)

type (
	MiddleWare[CTX ctx.IContextPtr[T], T any]                func(HandleFunc[CTX, T]) HandleFunc[CTX, T]
	HandleFunc[CTX ctx.IContextPtr[T], T any]                func(ctx CTX) *berror.ErrMsg
)

type Elem[CTX ctx.IContextPtr[T], T any] struct {
	h         HandleFunc[CTX, T]
	desc      string
	midS      []MiddleWare[CTX, T]
	isRPC     bool
	isRPCResp bool
	msgName   protoreflect.FullName
	funcType  string
	isForce   bool
	isSingle  bool
	reqPool   *sync.Pool
	respPool  *sync.Pool
}

func (this_ *Elem[CTX, T]) IsForce() bool {
	return this_.isForce
}

func (this_ *Elem[CTX, T]) MsgName() string {
	return string(this_.msgName)
}

func (this_ *Elem[CTX, T]) IsSingle() bool {
	return this_.isSingle
}

func (this_ *Elem[CTX, T]) IsRPC() bool {
	return this_.isRPC
}

func (this_ *Elem[CTX, T]) IsRPCResp() bool {
	return this_.isRPCResp
}

func (this_ *Elem[CTX, T]) ReqPool() *sync.Pool {
	return this_.reqPool
}

func (this_ *Elem[CTX, T]) RespPool() *sync.Pool {
	return this_.respPool
}

func (this_ *Elem[CTX, T]) String() string {
	if this_.isRPC {
		return fmt.Sprintf("[%s] func: %s", this_.desc, this_.funcType)
	}
	return fmt.Sprintf("[%s] rpc: %s", this_.desc, this_.funcType)
}

func (this_ *Elem[CTX, T]) Call(c CTX) *berror.ErrMsg {
	hf := this_.call(0, c)
	return hf(c)
}

func (this_ *Elem[CTX, T]) call(i int, c CTX) HandleFunc[CTX, T] {
	if i == len(this_.midS) {
		return this_.h
	}
	hf := this_.call(i+1, c)
	return this_.midS[i](hf)
}

type Handler[CTX ctx.IContextPtr[T], T any] struct {
	handle         map[protoreflect.FullName]*Elem[CTX, T]
	subjMap        map[string]struct{}
	baseMiddleware []MiddleWare[CTX, T]
	broadcastSubj  map[string]struct{}
}

// NewHandler create Router Handler
func NewHandler[CTX ctx.IContextPtr[T], T any](middlewares ...MiddleWare[CTX, T]) *Handler[CTX, T] {
	h := &Handler[CTX, T]{
		handle:        map[protoreflect.FullName]*Elem[CTX, T]{},
		subjMap:       map[string]struct{}{},
		broadcastSubj: map[string]struct{}{},
	}
	h.baseMiddleware = append(h.baseMiddleware, middlewares...)
	return h
}

// Group create a new Handler with same base middlewares
func (h *Handler[CTX, T]) Group(middlewares ...MiddleWare[CTX, T]) *Handler[CTX, T] {
	newH := &Handler[CTX, T]{
		handle:  h.handle,
		subjMap: h.subjMap,
	}
	newH.baseMiddleware = append(newH.baseMiddleware, h.baseMiddleware...)
	newH.baseMiddleware = append(newH.baseMiddleware, middlewares...)
	return newH
}

// GetHandler get Elem by msgName or nil
func (h *Handler[CTX, T]) GetHandler(msgName string) (*Elem[CTX, T], bool) {
	e, ok := h.handle[protoreflect.FullName(msgName)]
	return e, ok
}

// GetQueueSubjInfo get all queue subj topic prefix
func (h *Handler[CTX, T]) GetQueueSubjInfo() map[string]struct{} {
	return h.subjMap
}

// GetBroadcastSubjInfo get all broadcast subj topic prefix
func (h *Handler[CTX, T]) GetBroadcastSubjInfo() map[string]struct{} {
	return h.broadcastSubj
}

func (h *Handler[CTX, T]) Logger() {
	for _, e := range h.handle {
		logger.Log.Info().Str("func", e.String()).Msg("register handler")
	}
}

// RegisterRPC RESP is out parameter and Call in many goroutines,just one sub will receive the event
func RegisterRPC[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[CTX, T],
) {
	registerRPC(h, desc, f, false, false, middlewares...)
}

// RegisterRPCSingle RESP is out parameter and Call on eventloop,just one sub will receive the event
func RegisterRPCSingle[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[CTX, T],
) {
	registerRPC(h, desc, f, false, true, middlewares...)
}

// RegisterRPCForce f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterRPCForce[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[CTX, T],
) {
	registerRPC(h, desc, f, true, false, middlewares...)
}

// RegisterRPCForceSingle f is force handle when Server is busy on eventloop，just one sub will receive the event
func RegisterRPCForceSingle[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) (RESP, *berror.ErrMsg), middlewares ...MiddleWare[CTX, T],
) {
	registerRPC(h, desc, f, true, true, middlewares...)
}

func getSubjPrefix(msgName string) string {
	return msgName[:strings.LastIndexByte(msgName, '.')+1]
}

func registerRPC[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) (RESP, *berror.ErrMsg), isForceHandle, isSingle bool, middlewares ...MiddleWare[CTX, T],
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

	midS := make([]MiddleWare[CTX, T], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[CTX, T]{
		h: func(c CTX) *berror.ErrMsg {
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
		reqPool:  objectpool.GetTypePool[T1](),
		respPool: objectpool.GetTypePool[T2](),
	}
}

// RegisterRPCResp RESP is in parameter and Call in many goroutines，just one sub will receive the event
func RegisterRPCResp[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerRPCResp(h, desc, f, false, false, middlewares...)
}

// RegisterRPCRespSingle RESP is in parameter and Call in eventloop，just one sub will receive the event
func RegisterRPCRespSingle[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerRPCResp(h, desc, f, false, true, middlewares...)
}

// RegisterRPCRespForce f is force handle when Server is busy many goroutines
// It is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterRPCRespForce[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerRPCResp(h, desc, f, true, false, middlewares...)
}

// RegisterRPCRespForceSingle f is force handle when Server is busy on eventloop，just one sub will receive the event
func RegisterRPCRespForceSingle[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ, RESP) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerRPCResp(h, desc, f, true, true, middlewares...)
}

func registerRPCResp[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], T any, T1 any, T2 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ, RESP) *berror.ErrMsg, isForceHandle, isSingle bool, middlewares ...MiddleWare[CTX, T],
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

	midS := make([]MiddleWare[CTX, T], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	respPool := objectpool.GetTypePool[T2]()
	h.handle[msgName] = &Elem[CTX, T]{
		h: func(c CTX) *berror.ErrMsg {
			bc := c.MustBaseContext()
			resp := respPool.Get().(RESP)
			err := f(c, bc.Req.(REQ), resp)
			if err != nil {
				respPool.Put(resp)
				return err
			}

			bc.Resp = append(bc.Resp, resp)
			return nil
		},
		desc:      desc,
		midS:      midS,
		isRPC:     true,
		isRPCResp: true,
		msgName:   msgName,
		isForce:   isForceHandle,
		isSingle:  isSingle,
		funcType:  " [" + runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name() + "] " + reflect.TypeOf(f).String(),
		reqPool:   objectpool.GetTypePool[T1](),
		respPool:  respPool,
	}
}

// RegisterEvent Call in many goroutines, just one sub will receive the event
func RegisterEvent[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, false, false, false, middlewares...)
}

// RegisterEventSingle Call in eventloop, just one sub will receive the event
func RegisterEventSingle[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, false, true, false, middlewares...)
}

// RegisterEventForce f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterEventForce[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, true, false, false, middlewares...)
}

// RegisterEventForceSingle f is force handle when Server is busy on eventloop
// just one sub will receive the event
func RegisterEventForceSingle[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, true, true, false, middlewares...)
}

func registerEvent[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, isForceHandle, isSingle, isBroadcast bool, middlewares ...MiddleWare[CTX, T],
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

	midS := make([]MiddleWare[CTX, T], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[CTX, T]{
		h: func(c CTX) *berror.ErrMsg {
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
		reqPool:  objectpool.GetTypePool[T1](),
	}
}

// RegisterEventBroadcast Call in many goroutines,and all subscribers will receive
func RegisterEventBroadcast[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, false, false, true, middlewares...)
}

// RegisterEventSingleBroadcast Call in eventloop ,and all subscribers will receive
func RegisterEventSingleBroadcast[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, false, true, true, middlewares...)
}

// RegisterEventForceBroadcast f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// all subscribers will receive
func RegisterEventForceBroadcast[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, true, false, true, middlewares...)
}

// RegisterEventForceSingleBroadcast f is force handle when Server is busy on eventloop
// all subscribers will receive
func RegisterEventForceSingleBroadcast[CTX ctx.IContextPtr[T], REQ define.ProtoMessagePtr[T1], T any, T1 any](
	h *Handler[CTX, T], desc string, f func(CTX, REQ) *berror.ErrMsg, middlewares ...MiddleWare[CTX, T],
) {
	registerEvent(h, desc, f, true, true, true, middlewares...)
}
