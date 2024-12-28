package ctx

import (
	"context"
	"hash/crc32"
	"sync"
	"time"

	"github.com/ravinggo/game/common/logger"
	"github.com/ravinggo/game/common/utils"
)

type BaseContext struct {
	context.Context
	TraceLog *logger.Logger
}

func (c *BaseContext) Reset() {
	c.Context = context.TODO()
}

var ctxPool = sync.Pool{
	New: func() interface{} {
		c := &BaseContext{}
		return c
	},
}

func NewCtx() *BaseContext {
	return ctxPool.Get().(*BaseContext)
}

func (c *BaseContext) Release() {
	c.Reset()
	ctxPool.Put(c)
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
func Value[K comparable, V any](c *BaseContext, key K) (V, bool) {
	v := c.Value(key)
	if v != nil {
		if value, ok := v.(V); ok {
			return value, true
		}
	}

	var zero V
	return zero, false
}

type MarkToHash interface {
	// MarkToHash return a hash value for cluster router msg
	MarkToHash() int64
}

// MarkCtx is a context with a mark
// mark can be roleID,or roleInfo, or other
type MarkCtx[MARK MarkToHash] struct {
	BaseContext
	Mark MARK
}

// NewMarkCtx create a new MarkCtx
func NewMarkCtx[MARK MarkToHash](mark MARK) *MarkCtx[MARK] {
	c := &MarkCtx[MARK]{Mark: mark}
	return c
}

type Int64Mark int64

func (i Int64Mark) MarkToHash() int64 {
	return int64(i)
}

type StringMark string

func (i StringMark) MarkToHash() int64 {
	if len(i) == 0 {
		return 0
	}

	return int64(crc32.ChecksumIEEE(utils.StringToBytes(string(i))))
}

type IntMarkCtx MarkCtx[Int64Mark]
type StringMarkCtx MarkCtx[Int64Mark]
