// Package ctx provides the core request context and trace types for the game server framework.
// This framework exclusively supports int64 RoleID for user identification and routing.
package ctx

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/xid"
)

// Trace is the per-request trace contract carried through the distributed system.
// All routing is performed by int64 RoleID — the framework only supports int64 RoleID.
// Written by Claude Code claude-opus-4-6.
type Trace interface {
	// GetRoleID returns the int64 role identifier used for consistent message routing.
	// The same RoleID always routes to the same worker, enabling per-entity ordering without locks.
	// A zero value means the message has no ordering requirement.
	GetRoleID() int64
	// SetRoleID overrides the RoleID on a pooled context acquired inside taskFunc.
	// Used when Data is nil and a fresh context must carry the caller's RoleID.
	SetRoleID(int64)

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

// TracePtr combines the Trace interface with a pointer-to-T constraint so that generic
// functions can treat *T as a Trace without additional type assertions.
// Written by Claude Code claude-opus-4-6.
type TracePtr[T any] interface {
	Trace
	*T
}

// IContextPtr pairs IContext with a pointer-to-T constraint, enabling generic code to
// work with concrete context types without losing the ability to obtain a typed pointer.
// Written by Claude Code claude-opus-4-6.
type IContextPtr[T any] interface {
	IContext
	*T
}

// IContext is the primary context interface used throughout the framework. It composes
// the standard context.Context, the Clear (Reset) contract for object-pool reuse,
// mutable key-value storage via SetValue, access to the distributed trace via GetTrace,
// and structured logging via logger.ILogger.
// Written by Claude Code claude-opus-4-6.
type IContext interface {
	context.Context
	define.Clear
	SetValue(any, any)
	GetTrace() Trace
	logger.ILogger
}

// Value returns the value stored in c for the given key, together with a boolean that
// indicates whether a value of the expected type V was found. It is a typed wrapper
// around the standard context.Context.Value lookup and avoids callers having to
// perform manual type assertions.
// Written by Claude Code claude-opus-4-6.
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

// BaseCtx is the generic, pool-friendly request context used by every handler in the
// framework. TraceData holds the per-request trace payload (IntTrace) and TP is a
// pointer to TraceData that satisfies the Trace interface (int64 RoleID only).
// Fields are kept public so that service infrastructure can populate them directly;
// callers should treat the struct as mutable only during handler dispatch.
// Written by Claude Code claude-opus-4-6.
type BaseCtx[TraceData any, TP TracePtr[TraceData]] struct {
	Context   context.Context
	Req       proto.Message
	Resp      proto.Message   // RPC response, acquired from pool together with Req; nil for events
	OtherResp []proto.Message // additional messages appended to the reply alongside Resp

	// for rpc reply
	NatsMsg *nats.Msg
	TD      TraceData
}

// Deadline delegates to the embedded context.Context. Returns zero time and false when
// no inner context has been set.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Deadline() (deadline time.Time, ok bool) {
	if c.Context == nil {
		return
	}
	return c.Context.Deadline()
}

// Done delegates to the embedded context.Context. Returns nil when no inner context
// has been set, which signals that the context will never be cancelled.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Done() <-chan struct{} {
	if c.Context == nil {
		return nil
	}
	return c.Context.Done()
}

// Err delegates to the embedded context.Context. Returns nil when no inner context
// has been set.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Err() error {
	if c.Context == nil {
		return nil
	}
	return c.Context.Err()
}

// Value implements context.Context and returns the value associated with key from the
// embedded context chain. Returns nil when no inner context has been set.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Value(key any) any {
	if c.Context == nil {
		return nil
	}
	return c.Context.Value(key)
}

// SetValue stores a key-value pair in the context by wrapping the current inner context
// with context.WithValue. If no inner context exists one is created from context.Background.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) SetValue(key any, value any) {
	if c.Context == nil {
		c.Context = context.Background()
	}
	c.Context = context.WithValue(c.Context, key, value)
}

// Reset returns the context to a clean state suitable for reuse from an object pool.
// It reinstates a fresh background context, empties the response slice while retaining
// its allocated capacity, clears the NATS reply message, and delegates Reset to the
// embedded trace data via the TP pointer.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) Reset() {
	c.Context = context.Background()
	c.Req = nil
	c.Resp = nil
	clear(c.OtherResp)
	c.OtherResp = c.OtherResp[:0]
	c.NatsMsg = nil
	(TP)(&c.TD).Reset()
}

// GetTrace returns the Trace associated with this context by converting the embedded
// TraceData value to the TP pointer type. The returned Trace is never nil.
// Written by Claude Code claude-opus-4-6.
func (c *BaseCtx[TraceData, TP]) GetTrace() Trace {
	return (TP)(&c.TD)
}

// IntTrace extends the protobuf-generated basepb.IntTrace with framework-level behaviour:
// routing by int64 RoleId, automatic trace-ID generation on first marshal, and zerolog
// field attachment for structured logging. It is the only trace type supported by this framework.
// Written by Claude Code claude-opus-4-6.
type IntTrace struct {
	basepb.IntTrace
	id define.TraceID
}

// GetRoleID returns the int64 RoleId used for consistent per-entity message routing.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) GetRoleID() int64 { return i.RoleId }

// SetRoleID overrides the RoleId on a pooled context acquired inside taskFunc.
func (i *IntTrace) SetRoleID(roleID int64) { i.RoleId = roleID }

// Reset clears protobuf fields, preparing the object for pool reuse.
func (i *IntTrace) Reset() { i.IntTrace.Reset() }

// TraceMarshalSize returns the number of bytes required to serialise this trace.
// If no TraceId has been set, one is generated and stored before computing the size.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) TraceMarshalSize() int {
	if i.TraceId == "" && i.id.IsNil() {
		i.id = xid.NewIDString()
		i.TraceId = i.id.String()
	}
	return define.ProtoSize(i)
}

// TraceMarshalAppend serialises the trace and appends the bytes to b, returning the
// extended slice. A TraceId is generated on the first call if one has not been set.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) TraceMarshalAppend(b []byte) ([]byte, error) {
	if i.TraceId == "" && i.id.IsNil() {
		i.id = xid.NewIDString()
		i.TraceId = i.id.String()
	}
	return define.ProtoMarshalAppend(b, i)
}

// TraceMarshalFrom deserialises b into the trace, restoring all protobuf fields.
// If the decoded message contains a non-empty TraceId it is also copied into the
// internal fixed-size id field for fast comparison.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) TraceMarshalFrom(b []byte) error {
	err := define.ProtoUnmarshal(b, i)
	if err != nil {
		return err
	}
	if i.TraceId != "" {
		copy(i.id[:], i.TraceId)
	}

	return nil
}

// GetServerIdAndType returns the server ID and server type recorded in this trace,
// identifying the service that originated the request.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) GetServerIdAndType() (int64, string) {
	return i.FromServerId, i.FromServerType
}

// SetServerIdAndType records the originating server's numeric ID and type name in the
// trace so that downstream services can identify the caller.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) SetServerIdAndType(serverId int64, serverType string) {
	i.FromServerId = serverId
	i.FromServerType = serverType
}

// TraceLogField attaches trace-specific fields (traceId, fromServerId, fromServerType,
// roleId) to the zerolog Event, enabling consistent structured logging across services.
// Written by Claude Code claude-opus-4-6.
func (i *IntTrace) TraceLogField(e *logger.Event) *logger.Event {
	return e.Str("traceId", i.TraceId).Int64("fromServerId", i.FromServerId).
		Str("fromServerType", i.FromServerType).Int64("roleId", i.RoleId)
}

// Int64TraceCtx is the canonical context type for this framework.
// Only int64 RoleID is supported for routing.
type Int64TraceCtx = BaseCtx[IntTrace, *IntTrace]

var (
	_ Trace    = (*IntTrace)(nil)
	_ IContext = (*Int64TraceCtx)(nil)
)
