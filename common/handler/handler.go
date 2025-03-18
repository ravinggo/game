package handler

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
)

type (
	Middleware[TraceData any, TP ctx.TracePtr[TraceData]]                func(HandleFunc[TraceData, TP]) HandleFunc[TraceData, TP]
	HandleFunc[TraceData any, TP ctx.TracePtr[TraceData]]                func(*ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg
)

type Elem[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	h         HandleFunc[TraceData, TP]
	desc      string
	midS      []Middleware[TraceData, TP]
	isRPC     bool
	isRPCResp bool
	msgName   protoreflect.FullName
	funcType  string
	isForce   bool
	isSingle  bool
	reqPool   *sync.Pool
	respPool  *sync.Pool
}

func (this_ *Elem[TraceData, TP]) IsForce() bool {
	return this_.isForce
}

func (this_ *Elem[TraceData, TP]) MsgName() string {
	return string(this_.msgName)
}

func (this_ *Elem[TraceData, TP]) IsSingle() bool {
	return this_.isSingle
}

func (this_ *Elem[TraceData, TP]) IsRPC() bool {
	return this_.isRPC
}

func (this_ *Elem[TraceData, TP]) IsRPCResp() bool {
	return this_.isRPCResp
}

func (this_ *Elem[TraceData, TP]) ReqPool() *sync.Pool {
	return this_.reqPool
}

func (this_ *Elem[TraceData, TP]) RespPool() *sync.Pool {
	return this_.respPool
}

func (this_ *Elem[TraceData, TP]) String() string {
	if this_.isRPC {
		return fmt.Sprintf("[%s] func: %s", this_.desc, this_.funcType)
	}
	return fmt.Sprintf("[%s] rpc: %s", this_.desc, this_.funcType)
}

func (this_ *Elem[TraceData, TP]) Call(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
	hf := this_.call(0, c)
	return hf(c)
}

func (this_ *Elem[TraceData, TP]) call(i int, c *ctx.BaseCtx[TraceData, TP]) HandleFunc[TraceData, TP] {
	if i == len(this_.midS) {
		return this_.h
	}
	hf := this_.call(i+1, c)
	return this_.midS[i](hf)
}

type Handler[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	handle         map[protoreflect.FullName]*Elem[TraceData, TP]
	subjMap        map[string]struct{}
	baseMiddleware []Middleware[TraceData, TP]
	broadcastSubj  map[string]struct{}
}

// NewHandler create Router Handler
func NewHandler[TraceData any, TP ctx.TracePtr[TraceData]](middlewares ...Middleware[TraceData, TP]) *Handler[TraceData, TP] {
	h := &Handler[TraceData, TP]{
		handle:        map[protoreflect.FullName]*Elem[TraceData, TP]{},
		subjMap:       map[string]struct{}{},
		broadcastSubj: map[string]struct{}{},
	}
	h.baseMiddleware = append(h.baseMiddleware, middlewares...)
	return h
}

// Group create a new Handler with same base middlewares
func (h *Handler[TraceData, TP]) Group(middlewares ...Middleware[TraceData, TP]) *Handler[TraceData, TP] {
	newH := &Handler[TraceData, TP]{
		handle:  h.handle,
		subjMap: h.subjMap,
	}
	newH.baseMiddleware = append(newH.baseMiddleware, h.baseMiddleware...)
	newH.baseMiddleware = append(newH.baseMiddleware, middlewares...)
	return newH
}

// GetHandler get Elem by msgName or nil
func (h *Handler[TraceData, TP]) GetHandler(msgName string) (*Elem[TraceData, TP], bool) {
	e, ok := h.handle[protoreflect.FullName(msgName)]
	return e, ok
}

// GetQueueSubjInfo get all queue subj topic prefix
func (h *Handler[TraceData, TP]) GetQueueSubjInfo() map[string]struct{} {
	return h.subjMap
}

// GetBroadcastSubjInfo get all broadcast subj topic prefix
func (h *Handler[TraceData, TP]) GetBroadcastSubjInfo() map[string]struct{} {
	return h.broadcastSubj
}

func (h *Handler[TraceData, TP]) Logger() {
	for _, e := range h.handle {
		logger.Log.Info().Str("func", e.String()).Msg("register handler")
	}
}

// RegisterRPC RESP is out parameter and Call in many goroutines,just one sub will receive the event
func RegisterRPC[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) (RESP, *berror.ErrMsg), middlewares ...Middleware[TraceData, TP],
) {
	registerRPC(h, desc, f, false, false, middlewares...)
}

// RegisterRPCSingle RESP is out parameter and Call on eventloop,just one sub will receive the event
func RegisterRPCSingle[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) (RESP, *berror.ErrMsg), middlewares ...Middleware[TraceData, TP],
) {
	registerRPC(h, desc, f, false, true, middlewares...)
}

// RegisterRPCForce f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterRPCForce[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) (RESP, *berror.ErrMsg), middlewares ...Middleware[TraceData, TP],
) {
	registerRPC(h, desc, f, true, false, middlewares...)
}

// RegisterRPCForceSingle f is force handle when Server is busy on eventloop，just one sub will receive the event
func RegisterRPCForceSingle[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) (RESP, *berror.ErrMsg), middlewares ...Middleware[TraceData, TP],
) {
	registerRPC(h, desc, f, true, true, middlewares...)
}

func getSubjPrefix(msgName string) string {
	return msgName[:strings.LastIndexByte(msgName, '.')+1]
}

func registerRPC[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) (RESP, *berror.ErrMsg), isForceHandle, isSingle bool,
	middlewares ...Middleware[TraceData, TP],
) {
	var req REQ
	msgName := define.ProtoMessageName(req)
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if _, ok := h.broadcastSubj[subj]; ok {
			panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
		}
		h.subjMap[subj] = struct{}{}
	}

	midS := make([]Middleware[TraceData, TP], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[TraceData, TP]{
		h: func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
			c.Resp = append(c.Resp, nil)
			ret, err := f(c, c.Req.(REQ))
			if err != nil {
				return err
			}
			c.Resp[0] = ret
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
func RegisterRPCResp[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerRPCResp(h, desc, f, false, false, middlewares...)
}

// RegisterRPCRespSingle RESP is in parameter and Call in eventloop，just one sub will receive the event
func RegisterRPCRespSingle[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerRPCResp(h, desc, f, false, true, middlewares...)
}

// RegisterRPCRespForce f is force handle when Server is busy many goroutines
// It is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterRPCRespForce[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerRPCResp(h, desc, f, true, false, middlewares...)
}

// RegisterRPCRespForceSingle f is force handle when Server is busy on eventloop，just one sub will receive the event
func RegisterRPCRespForceSingle[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerRPCResp(h, desc, f, true, true, middlewares...)
}

func registerRPCResp[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], RESP define.ProtoMessagePtr[T2], TraceData any, T1 any, T2 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ, RESP) *berror.ErrMsg, isForceHandle, isSingle bool,
	middlewares ...Middleware[TraceData, TP],
) {
	var req REQ
	msgName := define.ProtoMessageName(req)
	if v, ok := h.handle[msgName]; ok {
		panic(fmt.Sprintf("Handler %s already registered![%s]", msgName, v.String()))
	} else {
		subj := getSubjPrefix(string(msgName))
		if _, ok := h.broadcastSubj[subj]; ok {
			panic(fmt.Sprintf("subj %s already registered as broadcast message,not is normal message[%s]", msgName, v.String()))
		}
		h.subjMap[subj] = struct{}{}
	}

	midS := make([]Middleware[TraceData, TP], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	respPool := objectpool.GetTypePool[T2]()
	h.handle[msgName] = &Elem[TraceData, TP]{
		h: func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
			resp := respPool.Get().(RESP)
			c.Resp = append(c.Resp, resp)
			err := f(c, c.Req.(REQ), resp)
			if err != nil {
				respPool.Put(resp)
				return err
			}

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
func RegisterEvent[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, false, false, false, middlewares...)
}

// RegisterEventSingle Call in eventloop, just one sub will receive the event
func RegisterEventSingle[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, false, true, false, middlewares...)
}

// RegisterEventForce f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// just one sub will receive the event
func RegisterEventForce[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, true, false, false, middlewares...)
}

// RegisterEventForceSingle f is force handle when Server is busy on eventloop
// just one sub will receive the event
func RegisterEventForceSingle[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, true, true, false, middlewares...)
}

func registerEvent[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, isForceHandle, isSingle, isBroadcast bool,
	middlewares ...Middleware[TraceData, TP],
) {
	var req REQ
	msgName := define.ProtoMessageName(req)
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

	midS := make([]Middleware[TraceData, TP], 0, len(h.baseMiddleware)+len(middlewares))
	midS = append(midS, h.baseMiddleware...)
	midS = append(midS, middlewares...)
	h.handle[msgName] = &Elem[TraceData, TP]{
		h: func(c *ctx.BaseCtx[TraceData, TP]) *berror.ErrMsg {
			return f(c, c.Req.(REQ))
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
func RegisterEventBroadcast[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, false, false, true, middlewares...)
}

// RegisterEventSingleBroadcast Call in eventloop ,and all subscribers will receive
func RegisterEventSingleBroadcast[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, false, true, true, middlewares...)
}

// RegisterEventForceBroadcast f is force handle when Server is busy many goroutines
// it is only used for very important businesses, such as recharge-related interfaces, adding gold coins to players, etc.
// all subscribers will receive
func RegisterEventForceBroadcast[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, true, false, true, middlewares...)
}

// RegisterEventForceSingleBroadcast f is force handle when Server is busy on eventloop
// all subscribers will receive
func RegisterEventForceSingleBroadcast[TP ctx.TracePtr[TraceData], REQ define.ProtoMessagePtr[T1], TraceData any, T1 any](
	h *Handler[TraceData, TP], desc string, f func(*ctx.BaseCtx[TraceData, TP], REQ) *berror.ErrMsg, middlewares ...Middleware[TraceData, TP],
) {
	registerEvent(h, desc, f, true, true, true, middlewares...)
}
