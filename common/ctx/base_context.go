package ctx

import (
	"context"
	"hash/crc32"
	"math/rand/v2"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/objectpool"
	"github.com/ravinggo/game/common/utils"
)

type canReset interface {
	Reset()
}

type IContext interface {
	context.Context
	Release()
	canReset
}

type IHashContext interface {
	IContext
	ToHash
}

type BaseContext struct {
	context.Context
	TraceLog *logger.Logger
	TraceID  string
}

func (c *BaseContext) reset() {
	c.Context = context.TODO()
	c.TraceID = ""
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

type ToHash interface {
	// ToHash return a hash value for cluster router msg
	ToHash() uint64
}

type MarshalCtx interface {
	TraceMarshalSize() int

	// TraceMarshalAppend marshal the object to byte slice
	TraceMarshalAppend([]byte) ([]byte, error)

	// TraceMarshalFrom unmarshal the object from byte slice
	TraceMarshalFrom([]byte) error
}

// MarkCtx is a context with a mark
// mark can be roleID,or roleInfo, or other
type MarkCtx[MARK any] struct {
	BaseContext
	Mark MARK
}

// NewMarkCtxWithMark create a new MarkCtx
func NewMarkCtxWithMark[MARK any](mark MARK) *MarkCtx[MARK] {
	c := objectpool.Get[MarkCtx[MARK]]()
	c.Mark = mark
	return c
}

// NewMarkCtx create a new MarkCtx
func NewMarkCtx[MARK any]() *MarkCtx[MARK] {
	c := objectpool.Get[MarkCtx[MARK]]()
	return c
}

func (c *MarkCtx[MARK]) Release() {
	c.Reset()
	objectpool.Put[MarkCtx[MARK]](c)
}

func (c *MarkCtx[MARK]) Reset() {
	var mark any = c.Mark
	if m, ok := mark.(canReset); ok {
		m.Reset()
	}
	c.reset()
}

type IntMark struct {
	basepb.IntMark
}

func (i *IntMark) ToHash() uint64 {
	if i.Mark != 0 {
		if i.Mark > 0 {
			return uint64(i.Mark)
		}
		return uint64(-i.Mark)
	}
	if i.FromServerId != 0 {
		if i.FromServerId > 0 {
			return uint64(i.FromServerId)
		}
		return uint64(-i.FromServerId)
	}

	return rand.Uint64()
}

func (i *IntMark) TraceMarshalSize() int {
	return proto.Size(i)
}

func (i *IntMark) TraceMarshalAppend(b []byte) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(b, i)
}

func (i *IntMark) TraceMarshalFrom(b []byte) error {
	return proto.Unmarshal(b, i)
}

type StringMark struct {
	basepb.StringMark
}

func (i *StringMark) ToHash() uint64 {
	if len(i.Mark) == 0 {
		if i.FromServerId != 0 {
			if i.FromServerId > 0 {
				return uint64(i.FromServerId)
			}
			return uint64(-i.FromServerId)
		}

		return rand.Uint64()
	}

	return uint64(crc32.ChecksumIEEE(utils.StringToBytes(i.Mark)))
}

func (i *StringMark) TraceMarshalSize() int {
	return proto.Size(i)
}

func (i *StringMark) TraceMarshalAppend(b []byte) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(b, i)
}

func (i *StringMark) TraceMarshalFrom(b []byte) error {
	return proto.Unmarshal(b, i)
}

type Int64MarkCtx MarkCtx[IntMark]
type StringMarkCtx MarkCtx[StringMark]

var (
	_ ToHash = (*IntMark)(nil)
	_ ToHash = (*StringMark)(nil)
)
