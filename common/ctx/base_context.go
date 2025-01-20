package ctx

import (
	"context"
	"hash/crc64"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/objectpool"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/utils"
)

type RoleIdType interface {
	int64 | string
}

type ToHash interface {
	// ToHash return a hash value for cluster router msg
	// uint64 is the hash value
	// one hash one goroutine if bool is true
	ToHash() uint64
}

type Trace interface {
	ToHash() uint64

	TraceMarshalSize() int
	// TraceMarshalAppend marshal the object to byte slice
	TraceMarshalAppend([]byte) ([]byte, error)
	// TraceMarshalFrom unmarshal the object from byte slice
	TraceMarshalFrom([]byte) error

	TraceLogField(logger.Context) logger.Context

	GetServerIdAndType() (int64, string)

	SetServerIdAndType(int64, string)

	Reset()
}

type TracePtr[T any] interface {
	Trace
	*T
}

type IContextPtr[T any] interface {
	IContext
	*T
}

type IContext interface {
	context.Context
	define.Clear
	SetValue(any, any)
	// MustBaseContext implementations of IContext must contain BaseContext
	MustBaseContext() *BaseContext
	GetTrace() Trace
}

type BaseContext struct {
	Context  context.Context
	TraceLog logger.Logger
	Req      proto.Message
	Resp     []proto.Message

	// for rpc reply
	NatsMsg *nats.Msg
}

func (c *BaseContext) GetTrace() Trace {
	return nil
}

func (c *BaseContext) MustBaseContext() *BaseContext {
	return c
}

func (c *BaseContext) Deadline() (deadline time.Time, ok bool) {
	if c.Context == nil {
		return
	}
	return c.Context.Deadline()
}

func (c *BaseContext) Done() <-chan struct{} {
	if c.Context == nil {
		return nil
	}
	return c.Context.Done()
}

func (c *BaseContext) Err() error {
	if c.Context == nil {
		return nil
	}
	return c.Context.Err()
}

func (c *BaseContext) Value(key any) any {
	if c.Context == nil {
		return nil
	}
	return c.Context.Value(key)
}

func (c *BaseContext) SetValue(key any, value any) {
	if c.Context == nil {
		c.Context = context.Background()
	}
	c.Context = context.WithValue(c.Context, key, value)
}

func (c *BaseContext) Reset() {
	c.Context = context.Background()
	clear(c.Resp)
	c.Resp = c.Resp[:0]
	c.NatsMsg = nil
}

var _ IContext = (*BaseContext)(nil)

// Value returns the value associated with this context for key, or nil
func Value[K comparable, V any](c IContext, key K) (V, bool) {
	v := c.Value(key)
	if v != nil {
		if value, ok := v.(V); ok {
			return value, true
		}
	}

	var zero V
	return zero, false
}

// TraceCtx is a context with a trace data,
// trace data can be roleID,or roleInfo, or other
type TraceCtx[TraceData any, TP TracePtr[TraceData]] struct {
	BaseContext
	TD TraceData
}

// NewMarkCtxWithMark create a new TraceCtx
func NewMarkCtxWithMark[TraceData any, TP TracePtr[TraceData]](traceData TraceData) *TraceCtx[TraceData, TP] {
	c := objectpool.Get[TraceCtx[TraceData, TP]]()
	c.TD = traceData
	return c
}

// NewMarkCtx create a new TraceCtx
func NewMarkCtx[TraceData any, TP TracePtr[TraceData]]() *TraceCtx[TraceData, TP] {
	c := objectpool.Get[TraceCtx[TraceData, TP]]()
	return c
}

func (c *TraceCtx[TraceData, TP]) Reset() {
	c.BaseContext.Reset()
	(TP)(&c.TD).Reset()
}

func (c *TraceCtx[TraceData, TP]) GetTrace() Trace {
	return (TP)(&c.TD)
}

//
// func (c *TraceCtx[TraceData, TP]) TraceMarshalSize() int {
// 	return (TP)(&c.TD).TraceMarshalSize()
// }
//
// func (c *TraceCtx[TraceData, TP]) TraceMarshalAppend(b []byte) ([]byte, error) {
// 	return (TP)(&c.TD).TraceMarshalAppend(b)
// }
//
// func (c *TraceCtx[TraceData, TP]) TraceMarshalFrom(b []byte) error {
// 	return (TP)(&c.TD).TraceMarshalFrom(b)
// }
//
// func (c *TraceCtx[TraceData, TP]) GetServerIdAndType() (int64, string) {
// 	return (TP)(&c.TD).GetServerIdAndType()
// }
//
// func (c *TraceCtx[TraceData, TP]) SetServerIdAndType(serverId int64, serverType string) {
// 	(TP)(&c.TD).SetServerIdAndType(serverId, serverType)
// }
//
// func (c *TraceCtx[TraceData, TP]) TraceLogField(zc logger.Context) logger.Context {
// 	return (TP)(&c.TD).TraceLogField(zc)
// }

type IntTrace struct {
	basepb.IntTrace
}

func (i *IntTrace) ToHash() uint64 {
	if i.RoleId != 0 {
		if i.RoleId > 0 {
			return uint64(i.RoleId)
		}
		return uint64(-i.RoleId)
	}

	return 0
}

func (i *IntTrace) TraceMarshalSize() int {
	return proto.Size(i)
}

func (i *IntTrace) TraceMarshalAppend(b []byte) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(b, i)
}

func (i *IntTrace) TraceMarshalFrom(b []byte) error {
	return proto.Unmarshal(b, i)
}

func (i *IntTrace) GetServerIdAndType() (int64, string) {
	return i.FromServerId, i.FromServerType
}

func (i *IntTrace) SetServerIdAndType(serverId int64, serverType string) {
	i.FromServerId = serverId
	i.FromServerType = serverType
}

func (i *IntTrace) TraceLogField(zc logger.Context) logger.Context {
	return zc.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Int64("roleId", i.RoleId)
}

type StringTrace struct {
	basepb.StringTrace
}

func (i *StringTrace) ToHash() uint64 {
	if i.RoleId != "" {
		return crc64.Checksum(utils.StringToBytes(i.RoleId), crc64.MakeTable(crc64.ECMA))
	}

	return 0
}

func (i *StringTrace) Reset() {
	i.StringTrace.Reset()
}

func (i *StringTrace) TraceMarshalSize() int {
	return proto.Size(i)
}

func (i *StringTrace) TraceMarshalAppend(b []byte) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(b, i)
}

func (i *StringTrace) TraceMarshalFrom(b []byte) error {
	return proto.Unmarshal(b, i)
}

func (i *StringTrace) GetServerIdAndType() (int64, string) {
	return i.FromServerId, i.FromServerType
}

func (i *StringTrace) SetServerIdAndType(serverId int64, serverType string) {
	i.FromServerId = serverId
	i.FromServerType = serverType
}

func (i *StringTrace) TraceLogField(zc logger.Context) logger.Context {
	zc.Reset()
	return zc.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Str("roleId", i.RoleId)
}

type Int64TraceCtx = TraceCtx[IntTrace, *IntTrace]
type StringTraceCtx = TraceCtx[StringTrace, *StringTrace]

var (
	_ ToHash = (*IntTrace)(nil)
	_ ToHash = (*StringTrace)(nil)

	_ Trace = (*IntTrace)(nil)
	_ Trace = (*StringTrace)(nil)

	_ IContext = (*Int64TraceCtx)(nil)
	_ IContext = (*StringTraceCtx)(nil)
)
