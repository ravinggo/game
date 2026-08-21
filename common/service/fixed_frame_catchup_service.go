package service

import (
	"time"

	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
)

// FixedFrameCatchUpService runs all messages and a per-frame callback on a
// single goroutine whose target frame count is derived from elapsed run time:
// target = (now - startTime) / interval. Whenever the executed frame count is
// below target one more frame runs immediately — even when busy — so every
// logical frame is executed exactly once with no skipping, and a stall is
// paid back in full as a catch-up burst. Prefer this for lockstep-style logic
// where losing frames is unacceptable.
type FixedFrameCatchUpService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	fixedFrameService[TraceData, TP]
	loop *eventloop.CatchUpFrameLoop
}

// NewFixedFrameCatchUpService creates a fixed-frame service running at fps
// frames per second with elapsed-time catch-up pacing. Panics if fps is not
// positive.
func NewFixedFrameCatchUpService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	fps int,
	c Config[TraceData, TP],
) *FixedFrameCatchUpService[TraceData, TP] {
	loop := eventloop.NewCatchUpFrameLoop(fps, c.LockQueueThread)
	base := NewBaseService[TraceData, TP](
		natsUrls,
		func(c *ctx.BaseCtx[TraceData, TP], elem *handler.Elem[TraceData, TP]) {
			loop.PostEventQueue(CE[TraceData, TP]{Ctx: c, Elem: elem})
		},
		c,
	)
	s := &FixedFrameCatchUpService[TraceData, TP]{
		fixedFrameService: fixedFrameService[TraceData, TP]{
			BaseService: base,
			fl:          loop,
		},
		loop: loop,
	}
	return s
}

// FrameInterval returns the fixed frame duration (1s / fps).
func (s *FixedFrameCatchUpService[TraceData, TP]) FrameInterval() time.Duration {
	return s.loop.Interval()
}
