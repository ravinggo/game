package timer

import (
	"time"
)

// AfterFunc is similar to time.AfterFunc
func AfterFunc(duration time.Duration, f func()) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.afterFunc(duration, f)
}

// UntilFunc Until the specified time executes function f
func UntilFunc(t time.Time, f func()) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.untilFunc(t, f)
}

// Ticker Execute function f every interval duration
func Ticker(duration time.Duration, f func() bool) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.tickFunc(duration, f)
}

// DayPass Calculate how many days have passed between begin and end
func DayPass(begin time.Time, end time.Time) int64 {
	b := BeginOfDay(begin)
	e := BeginOfDay(end)
	d := int64(time.Hour * 24)
	return (e.UnixNano() - b.UnixNano()) / d
}

// BeginOfDay returns the beginning of the day
func BeginOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

// EndOfDay returns the end of the day
func EndOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 23, 59, 59, 999999999, t.Location())
}

// NextDay Return to the same time of the next day
func NextDay(t time.Time) time.Time {
	d := time.Hour * 24
	return t.Add(d)
}

// LastDay  Return to the same time on the previous day
func LastDay(t time.Time) time.Time {
	d := time.Hour * -24
	return t.Add(d)
}

// BeginOfWeek Return the beginning of the week
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

// BeginOfMouth returns the beginning of the month
func BeginOfMouth(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
}

// EndOfWeek returns the end of the week
func EndOfWeek(t time.Time) time.Time {
	wd := t.Weekday()
	day := 7 - wd
	duration := time.Hour * time.Duration(day) * 24
	w := EndOfDay(t)
	w = w.Add(duration)
	return w
}

// TimeStampToString Convert timestamp to string
func TimeStampToString(nTimer int64) string {
	return time.Unix(nTimer, 0).Format(time.DateTime)
}

// SecondToTime Convert seconds to hour, minute, second
func SecondToTime(second int64) (h int64, m int64, s int64) {
	h = second / 3600
	second = second % 3600
	m = second / 60
	s = second % 60
	return
}

// MillisecondToTime Convert milliseconds to hour, minute, second
func MillisecondToTime(millisecond int64) (h int64, m int64, s int64) {
	return SecondToTime(millisecond / 1000)
}
