package timer

import (
	"time"
)

// AfterFunc schedules f to run in a new goroutine after the given duration,
// mirroring the standard library's time.AfterFunc but using a pooled
// tickerElem to reduce allocation pressure. If f is nil the call is a no-op.
// Written by Claude Code claude-opus-4-6.
func AfterFunc(duration time.Duration, f func()) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.afterFunc(duration, f)
}

// UntilFunc schedules f to run in a new goroutine at wall-clock time t.
// If t is already in the past, f fires within one millisecond. If f is nil
// the call is a no-op.
// Written by Claude Code claude-opus-4-6.
func UntilFunc(t time.Time, f func()) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.untilFunc(t, f)
}

// Ticker invokes f repeatedly at the given interval. f must return true to
// continue ticking and false to stop. The first invocation happens after one
// interval, not immediately. If f is nil the call is a no-op.
// Written by Claude Code claude-opus-4-6.
func Ticker(duration time.Duration, f func() bool) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.tickFunc(duration, f)
}

// DayPass returns the number of calendar days that have elapsed between begin
// and end, measured from midnight of each day so partial days at the ends are
// counted correctly. The result is negative when end is before begin.
// Written by Claude Code claude-opus-4-6.
func DayPass(begin time.Time, end time.Time) int64 {
	b := BeginOfDay(begin)
	e := BeginOfDay(end)
	d := int64(time.Hour * 24)
	return (e.UnixNano() - b.UnixNano()) / d
}

// BeginOfDay returns a Time representing 00:00:00.000000000 on the same
// calendar day and in the same location as t.
// Written by Claude Code claude-opus-4-6.
func BeginOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

// EndOfDay returns a Time representing 23:59:59.999999999 on the same
// calendar day and in the same location as t.
// Written by Claude Code claude-opus-4-6.
func EndOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 23, 59, 59, 999999999, t.Location())
}

// NextDay returns a Time exactly 24 hours after t, preserving the time-of-day
// and location. Use this for simple arithmetic; for DST-aware "tomorrow at the
// same clock time" use AddDate(0, 0, 1) instead.
// Written by Claude Code claude-opus-4-6.
func NextDay(t time.Time) time.Time {
	d := time.Hour * 24
	return t.Add(d)
}

// LastDay returns a Time exactly 24 hours before t, preserving the
// time-of-day and location.
// Written by Claude Code claude-opus-4-6.
func LastDay(t time.Time) time.Time {
	d := time.Hour * -24
	return t.Add(d)
}

// BeginOfWeek returns a Time representing 00:00:00 on the Monday of the week
// that contains t, in the same location as t. Weeks start on Monday following
// the ISO-8601 convention.
// Written by Claude Code claude-opus-4-6.
func BeginOfWeek(t time.Time) time.Time {
	t = BeginOfDay(t)
	wd := int(t.Weekday())

	weekStartDay := int(time.Monday)
	if wd < weekStartDay {
		wd = wd + 7 - weekStartDay
	} else {
		wd = wd - weekStartDay
	}

	return t.AddDate(0, 0, -wd)
}

// BeginOfMouth returns a Time representing 00:00:00 on the first day of the
// month containing t, in the same location as t.
// Written by Claude Code claude-opus-4-6.
func BeginOfMouth(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
}

// EndOfWeek returns a Time representing 23:59:59.999999999 on the Sunday of
// the week that contains t, in the same location as t.
// Written by Claude Code claude-opus-4-6.
func EndOfWeek(t time.Time) time.Time {
	wd := t.Weekday()
	day := 7 - wd
	duration := time.Hour * time.Duration(day) * 24
	w := EndOfDay(t)
	w = w.Add(duration)
	return w
}

// TimeStampToString converts a Unix timestamp (seconds since epoch) to a
// human-readable string formatted as "2006-01-02 15:04:05" in the local
// timezone.
// Written by Claude Code claude-opus-4-6.
func TimeStampToString(nTimer int64) string {
	return time.Unix(nTimer, 0).Format(time.DateTime)
}

// SecondToTime decomposes a duration expressed in whole seconds into its
// constituent hours, minutes, and seconds components. Hours are not capped at
// 23 — values larger than 86399 yield h > 23.
// Written by Claude Code claude-opus-4-6.
func SecondToTime(second int64) (h int64, m int64, s int64) {
	h = second / 3600
	second = second % 3600
	m = second / 60
	s = second % 60
	return
}

// MillisecondToTime converts a millisecond duration to hours, minutes, and
// seconds by truncating to the nearest second and delegating to SecondToTime.
// Written by Claude Code claude-opus-4-6.
func MillisecondToTime(millisecond int64) (h int64, m int64, s int64) {
	return SecondToTime(millisecond / 1000)
}
