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
	"github.com/ravinggo/game/common/xid"
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

	TraceLogField(*logger.Event) *logger.Event

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
	GetTrace() Trace
	logger.ILogger
}

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

// BaseCtx is a context with a trace data,
// trace data can be roleID,or roleInfo, or other
type BaseCtx[TraceData any, TP TracePtr[TraceData]] struct {
	Context context.Context
	Req     proto.Message
	Resp    []proto.Message

	// for rpc reply
	NatsMsg *nats.Msg
	TD      TraceData
}

// NewMarkCtxWithMark create a new BaseCtx
func NewMarkCtxWithMark[TraceData any, TP TracePtr[TraceData]](traceData TraceData) *BaseCtx[TraceData, TP] {
	c := objectpool.Get[BaseCtx[TraceData, TP]]()
	c.TD = traceData
	return c
}

// NewMarkCtx create a new BaseCtx
func NewMarkCtx[TraceData any, TP TracePtr[TraceData]]() *BaseCtx[TraceData, TP] {
	c := objectpool.Get[BaseCtx[TraceData, TP]]()
	return c
}

func (c *BaseCtx[TraceData, TP]) Deadline() (deadline time.Time, ok bool) {
	if c.Context == nil {
		return
	}
	return c.Context.Deadline()
}

func (c *BaseCtx[TraceData, TP]) Done() <-chan struct{} {
	if c.Context == nil {
		return nil
	}
	return c.Context.Done()
}

func (c *BaseCtx[TraceData, TP]) Err() error {
	if c.Context == nil {
		return nil
	}
	return c.Context.Err()
}

func (c *BaseCtx[TraceData, TP]) Value(key any) any {
	if c.Context == nil {
		return nil
	}
	return c.Context.Value(key)
}

func (c *BaseCtx[TraceData, TP]) SetValue(key any, value any) {
	if c.Context == nil {
		c.Context = context.Background()
	}
	c.Context = context.WithValue(c.Context, key, value)
}

func (c *BaseCtx[TraceData, TP]) Reset() {
	c.Context = context.Background()
	clear(c.Resp)
	c.Resp = c.Resp[:0]
	c.NatsMsg = nil
	(TP)(&c.TD).Reset()
}

func (c *BaseCtx[TraceData, TP]) GetTrace() Trace {
	return (TP)(&c.TD)
}

type IntTrace struct {
	basepb.IntTrace
	id define.TraceID
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
	if i.TraceId == "" {
		i.id = xid.NewIDString()
		i.TraceId = i.id.String()
	}
	return proto.Size(i)
}

func (i *IntTrace) TraceMarshalAppend(b []byte) ([]byte, error) {
	if i.TraceId == "" {
		i.id = xid.NewIDString()
		i.TraceId = i.id.String()
	}
	return proto.MarshalOptions{}.MarshalAppend(b, i)
}

func (i *IntTrace) TraceMarshalFrom(b []byte) error {
	err := proto.Unmarshal(b, i)
	if err != nil {
		return err
	}
	if i.TraceId != "" {
		copy(i.id[:], i.TraceId)
	}

	return nil
}

func (i *IntTrace) GetServerIdAndType() (int64, string) {
	return i.FromServerId, i.FromServerType
}

func (i *IntTrace) SetServerIdAndType(serverId int64, serverType string) {
	i.FromServerId = serverId
	i.FromServerType = serverType
}

func (i *IntTrace) TraceLogField(e *logger.Event) *logger.Event {
	return e.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Int64("roleId", i.RoleId)
}

type StringTrace struct {
	basepb.StringTrace
	id define.TraceID
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
	if i.TraceId == "" {
		i.id = xid.NewIDString()
		i.TraceId = i.id.String()
	}
	return proto.Size(i)
}

func (i *StringTrace) TraceMarshalAppend(b []byte) ([]byte, error) {
	if i.TraceId == "" {
		i.id = xid.NewIDString()
		i.TraceId = i.id.String()
	}
	return proto.MarshalOptions{}.MarshalAppend(b, i)
}

func (i *StringTrace) TraceMarshalFrom(b []byte) error {
	err := proto.Unmarshal(b, i)
	if err != nil {
		return err
	}
	if i.TraceId != "" {
		copy(i.id[:], i.TraceId)
	}

	return nil
}

func (i *StringTrace) GetServerIdAndType() (int64, string) {
	return i.FromServerId, i.FromServerType
}

func (i *StringTrace) SetServerIdAndType(serverId int64, serverType string) {
	i.FromServerId = serverId
	i.FromServerType = serverType
}

func (i *StringTrace) TraceLogField(e *logger.Event) *logger.Event {
	return e.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Str("roleId", i.RoleId)
}

type Int64TraceCtx = BaseCtx[IntTrace, *IntTrace]
type StringTraceCtx = BaseCtx[StringTrace, *StringTrace]

var (
	_ ToHash = (*IntTrace)(nil)
	_ ToHash = (*StringTrace)(nil)

	_ Trace = (*IntTrace)(nil)
	_ Trace = (*StringTrace)(nil)

	_ IContext = (*Int64TraceCtx)(nil)
	_ IContext = (*StringTraceCtx)(nil)
)
