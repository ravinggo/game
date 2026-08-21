package service

import (
	"time"

	"github.com/ravinggo/game/common/ctx"
	"github.com/ravinggo/game/common/eventloop"
	"github.com/ravinggo/game/common/handler"
)

// FixedFrameAbsService runs all messages and a per-frame callback on a single
// goroutine paced against absolute time: frame N is due at startTime +
// N*interval. Late frames are caught up by running back-to-back, so the
// long-run average rate is exactly fps and the frame delta is a deterministic
// fixed timestep. Prefer this for simulation/battle logic where logical time
// must not drift from wall time.
type FixedFrameAbsService[TraceData any, TP ctx.TracePtr[TraceData]] struct {
	fixedFrameService[TraceData, TP]
	loop *eventloop.AbsFrameLoop
}

// NewFixedFrameAbsService creates a fixed-frame service running at fps frames
// per second with absolute-time pacing. Panics if fps is not positive.
func NewFixedFrameAbsService[TraceData any, TP ctx.TracePtr[TraceData]](
	natsUrls []string,
	fps int,
	c Config[TraceData, TP],
) *FixedFrameAbsService[TraceData, TP] {
	base := NewBaseService[TraceData, TP](natsUrls, c)
	base.h = handler.NewHandler[TraceData](c.allMiddlewares()...)
	loop := eventloop.NewAbsFrameLoop(fps, c.LockQueueThread)
	s := &FixedFrameAbsService[TraceData, TP]{
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

// SetMaxCatchUpFrames overrides how far behind schedule the loop may fall
// before skipping the lost frames instead of catching up one by one.
// Must be called before Start; n <= 0 is ignored.
func (s *FixedFrameAbsService[TraceData, TP]) SetMaxCatchUpFrames(n int64) {
	s.loop.SetMaxCatchUpFrames(n)
}

// FrameInterval returns the fixed frame duration (1s / fps).
func (s *FixedFrameAbsService[TraceData, TP]) FrameInterval() time.Duration {
	return s.loop.Interval()
}
