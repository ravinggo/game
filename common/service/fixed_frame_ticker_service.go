package service

import (
	"time"

	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
)

// FixedFrameTickerService runs all messages and a per-frame callback on a
// single goroutine paced by a time.Ticker — the most common game-loop
// implementation. When a frame overruns the interval the missed ticks are
// dropped rather than caught up, so the effective rate slips below fps under
// sustained load. The frame delta is the actual elapsed time since the
// previous frame. Prefer this when occasional frame slip is acceptable and
// catch-up bursts are not wanted.
type FixedFrameTickerService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	fixedFrameService[TraceData, TP]
	loop *eventloop.TickerFrameLoop
}

// NewFixedFrameTickerService creates a fixed-frame service running at fps
// frames per second with ticker pacing. Panics if fps is not positive.
func NewFixedFrameTickerService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	fps int,
	c Config[TraceData, TP],
) *FixedFrameTickerService[TraceData, TP] {
	base := NewBaseService[TraceData, TP](natsUrls, c)
	base.h = handler.NewHandler[TraceData](c.allMiddlewares()...)
	loop := eventloop.NewTickerFrameLoop(fps, c.LockQueueThread)
	s := &FixedFrameTickerService[TraceData, TP]{
		fixedFrameService: fixedFrameService[TraceData, TP]{
			BaseService: base,
			fl:          loop,
		},
		loop: loop,
	}
	s.dispatch = func(cc *ctx.BaseCtx[TraceData, TP], elem *handler.Elem[TraceData, TP]) {
		loop.PostEventQueue(CE[TraceData, TP]{Ctx: cc, Elem: elem})
	}
	return s
}

// FrameInterval returns the fixed frame duration (1s / fps).
func (s *FixedFrameTickerService[TraceData, TP]) FrameInterval() time.Duration {
	return s.loop.Interval()
}
