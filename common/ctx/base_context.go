package ctx

import (
	"context"
	"hash/crc32"
	"math/rand/v2"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/utils"
)

type RoleIdType interface {
	int64 | string
}

type Clear interface {
	Clear()
}

type ToHash interface {
	// ToHash return a hash value for cluster router msg
	ToHash() uint64
}

type Trace interface {
	TraceMarshalSize() int

	// TraceMarshalAppend marshal the object to byte slice
	TraceMarshalAppend([]byte) ([]byte, error)

	// TraceMarshalFrom unmarshal the object from byte slice
	TraceMarshalFrom([]byte) error

	TraceLogField(*zerolog.Context)

	GetServerIdAndType() (int64, string)
	SetServerIdAndType(int64, string)
}

type IContext interface {
	context.Context
	Release()
	Clear
	ToHash

	// MustBaseContext implementations of IContext must contain BaseContext
	MustBaseContext() *BaseContext
}

type BaseContext struct {
	context.Context
	TraceLog logger.Logger
	Req      proto.Message
	Resp     []proto.Message

	// for rpc reply
	NatsMsg *nats.Msg
}

func (c *BaseContext) MustBaseContext() *BaseContext {
	return c
}

func (c *BaseContext) ToHash() uint64 {
	return rand.Uint64()
}

func (c *BaseContext) reset() {
	c.Context = context.TODO()
	logger.Log.With()
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

func (c *BaseContext) Clear() {
	c.TraceLog.UpdateContext(
		func(c zerolog.Context) zerolog.Context {
			return c
		},
	)
	c.Context = context.Background()
}

func (c *BaseContext) Release() {

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
type TraceCtx[TraceData any] struct {
	BaseContext
	TD TraceData
}

// NewMarkCtxWithMark create a new TraceCtx
func NewMarkCtxWithMark[TraceData any](traceData TraceData) *TraceCtx[TraceData] {
	c := objectpool.Get[TraceCtx[TraceData]]()
	c.TD = traceData
	return c
}

// NewMarkCtx create a new TraceCtx
func NewMarkCtx[TraceData any]() *TraceCtx[TraceData] {
	c := objectpool.Get[TraceCtx[TraceData]]()
	return c
}

func (c *TraceCtx[TraceData]) Release() {
	c.Clear()
	objectpool.Put[TraceCtx[TraceData]](c)
}

func (c *TraceCtx[TraceData]) Clear() {
	c.BaseContext.Clear()
	var mark any = c.TD
	if m, ok := mark.(Clear); ok {
		m.Clear()
	}
	c.reset()
}

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

	return rand.Uint64()
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

func (i *IntTrace) TraceLogField(zc *zerolog.Context) {
	zc.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Int64("roleId", i.RoleId)
}

type StringTrace struct {
	basepb.StringTrace
}

func (i *StringTrace) ToHash() uint64 {
	if i.RoleId != "" {
		return uint64(crc32.ChecksumIEEE(utils.StringToBytes(i.RoleId)))
	}

	return rand.Uint64()
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

func (i *StringTrace) TraceLogField(zc *zerolog.Context) {
	zc.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Str("roleId", i.RoleId)
}

type Int64TraceCtx TraceCtx[IntTrace]
type StringTraceCtx TraceCtx[StringTrace]

var (
	_ ToHash = (*IntTrace)(nil)
	_ ToHash = (*StringTrace)(nil)

	_ Trace = (*IntTrace)(nil)
	_ Trace = (*StringTrace)(nil)

	_ IContext = (*Int64TraceCtx)(nil)
	_ IContext = (*StringTraceCtx)(nil)
)
