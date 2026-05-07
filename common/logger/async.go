package logger

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// AsyncSink write log asynchronously
// Written by Claude Code claude-opus-4-6.
type AsyncSink struct {
	writer io.Writer

	swapBuff [2][]*bs
	cond     *sync.Cond

	close_ uint32
	closed chan struct{}
}

// NewAsync create a AsyncSink
// Written by Claude Code claude-opus-4-6.
func NewAsync(writer io.Writer) *AsyncSink {
	as := &AsyncSink{
		writer:   writer,
		swapBuff: [2][]*bs{},
		cond:     sync.NewCond(&sync.Mutex{}),
		closed:   make(chan struct{}, 1),
	}

	as.run()
	return as
}

const buffLen = 40960

var buffPool = sync.Pool{
	New: func() interface{} {
		return &bs{data: make([]byte, 0, buffLen)}
	},
}

// bs is a pooled byte-slice container used by AsyncSink to amortise allocations
// across many small log writes.
// Written by Claude Code claude-opus-4-6.
type bs struct {
	data []byte
}

// getBuff acquires a pooled bs instance from buffPool.
// Written by Claude Code claude-opus-4-6.
func getBuff() *bs {
	return buffPool.Get().(*bs)
}

// putBuff returns a bs to the pool if its capacity is within the pool limit;
// oversized buffers are discarded to prevent memory growth.
// Written by Claude Code claude-opus-4-6.
func putBuff(b *bs) {
	if cap(b.data) > buffLen {
		return
	}
	b.data = b.data[:0]
	buffPool.Put(b)
}

// Closed return true if closed
// Written by Claude Code claude-opus-4-6.
func (this_ *AsyncSink) Closed() bool {
	return atomic.LoadUint32(&this_.close_) == 1
}

// Close signals the background goroutine to flush remaining buffered data,
// waits for it to finish, and then closes the underlying writer if it implements
// io.Closer.
// Written by Claude Code claude-opus-4-6.
func (this_ *AsyncSink) Close() error {
	atomic.StoreUint32(&this_.close_, 1)
	this_.cond.Signal()
	<-this_.closed
	closer, ok := this_.writer.(io.Closer)
	if ok {
		return closer.Close()
	}

	return nil
}

var count int

// run starts the background goroutine that drains the double-buffer and writes
// accumulated log data to the underlying writer. It recovers from panics and
// restarts itself to prevent silent log loss.
// Written by Claude Code claude-opus-4-6.
func (this_ *AsyncSink) run() {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				_, _ = fmt.Fprintln(os.Stderr, "panic:", e)
				this_.run()
			}
		}()
		for !this_.Closed() {
			this_.cond.L.Lock()
			this_.swapBuff[0], this_.swapBuff[1] = this_.swapBuff[1], this_.swapBuff[0]
			for len(this_.swapBuff[1]) == 0 && !this_.Closed() {
				this_.cond.Wait()
				this_.swapBuff[0], this_.swapBuff[1] = this_.swapBuff[1], this_.swapBuff[0]
			}
			this_.cond.L.Unlock()

			for _, v := range this_.swapBuff[1] {
				count += len(v.data)
				_, err := this_.writer.Write(v.data)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
				}
				putBuff(v)
			}
			clear(this_.swapBuff[1])
			capSize := cap(this_.swapBuff[1])
			if capSize > 2560 {
				if len(this_.swapBuff[1]) < capSize/2 {
					this_.swapBuff[1] = make([]*bs, capSize/2)
				}
			}
			this_.swapBuff[1] = this_.swapBuff[1][:0]
		}
		for i, buff := range this_.swapBuff {
			for _, v := range buff {
				_, err := this_.writer.Write(v.data)
				if err != nil {
					_, _ = fmt.Fprintln(os.Stderr, err)
				}
				putBuff(v)
			}
			clear(buff)
			this_.swapBuff[i] = this_.swapBuff[i][:0]
		}
		close(this_.closed)
	}()
}

// Write buffers p for asynchronous delivery to the underlying writer. If the
// last pending buffer has room for p it is appended there to reduce the number
// of discrete write calls; otherwise a new buffer is acquired from the pool.
// Writes larger than buffLen bypass the pool entirely. Write is safe for
// concurrent use.
// Written by Claude Code claude-opus-4-6.
func (this_ *AsyncSink) Write(p []byte) (n int, err error) {
	lp := len(p)
	this_.cond.L.Lock()
	defer func() {
		this_.cond.L.Unlock()
		this_.cond.Signal()
	}()
	temp := this_.swapBuff[0]
	l := len(temp)
	if l > 0 {
		last := temp[l-1]
		if cap(last.data)-len(last.data) > lp {
			last.data = append(last.data, p...)
			return lp, nil
		}
	}
	if lp > buffLen {
		this_.swapBuff[0] = append(this_.swapBuff[0], &bs{data: p})
		return lp, nil
	}
	b := getBuff()
	b.data = append(b.data, p...)
	this_.swapBuff[0] = append(this_.swapBuff[0], b)
	return lp, nil
}
