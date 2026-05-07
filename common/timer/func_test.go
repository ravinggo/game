// Written by Claude Code claude-opus-4-6.
package timer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBeginOfWeek(t *testing.T) {
	r := require.New(t)
	start := time.Date(2022, 11, 28, 0, 0, 0, 0, time.UTC)
	tt := time.Date(2022, 11, 30, 0, 0, 0, 0, time.UTC)
	t1 := BeginOfWeek(tt)
	r.Equal(start, t1)

	tt = time.Date(2022, 11, 30, 13, 26, 0, 0, time.UTC)
	t1 = BeginOfWeek(tt)
	r.Equal(start, t1)

	tt = time.Date(2022, 12, 4, 23, 59, 59, 999, time.UTC)
	t1 = BeginOfWeek(tt)
	r.Equal(start, t1)

	tt = time.Date(2022, 12, 4, 23, 59, 60, 0, time.UTC)
	t1 = BeginOfWeek(tt)
	r.NotEqual(start, t1)
}

func TestSecondToTime(t *testing.T) {
	r := require.New(t)

	h, m, s := SecondToTime(100)
	r.EqualValues(h, 0)
	r.EqualValues(m, 1)
	r.EqualValues(s, 40)

	h, m, s = SecondToTime(3700)
	r.EqualValues(h, 1)
	r.EqualValues(m, 1)
	r.EqualValues(s, 40)

	h, m, s = SecondToTime(86401)
	r.EqualValues(h, 24)
	r.EqualValues(m, 0)
	r.EqualValues(s, 1)
}

// TestStartAndGetLowPrecisionTime verifies that after StartLowPrecisionTime is
// called, GetLowPrecisionTime returns a non-zero Unix timestamp.
func TestStartAndGetLowPrecisionTime(t *testing.T) {
	StartLowPrecisionTime()
	got := GetLowPrecisionTime()
	if got == 0 {
		t.Fatal("GetLowPrecisionTime returned 0 after StartLowPrecisionTime")
	}
	// The cached value should be within a couple of seconds of the real clock.
	now := time.Now().Unix()
	diff := now - got
	if diff < 0 {
		diff = -diff
	}
	if diff > 2 {
		t.Fatalf("GetLowPrecisionTime is %d seconds off from time.Now()", diff)
	}
}

// TestBeginOfDay verifies that BeginOfDay returns midnight of the given day.
func TestBeginOfDay(t *testing.T) {
	r := require.New(t)
	now := time.Now().UTC()
	bod := BeginOfDay(now)
	r.Equal(now.Year(), bod.Year())
	r.Equal(now.Month(), bod.Month())
	r.Equal(now.Day(), bod.Day())
	r.Equal(0, bod.Hour())
	r.Equal(0, bod.Minute())
	r.Equal(0, bod.Second())
	r.Equal(0, bod.Nanosecond())
}

// TestEndOfDay verifies that EndOfDay returns one nanosecond before midnight.
func TestEndOfDay(t *testing.T) {
	r := require.New(t)
	now := time.Now().UTC()
	eod := EndOfDay(now)
	r.Equal(now.Year(), eod.Year())
	r.Equal(now.Month(), eod.Month())
	r.Equal(now.Day(), eod.Day())
	r.Equal(23, eod.Hour())
	r.Equal(59, eod.Minute())
	r.Equal(59, eod.Second())
	r.Equal(999999999, eod.Nanosecond())
}

// TestNextDay verifies that NextDay returns a time exactly 24 hours later.
func TestNextDay(t *testing.T) {
	r := require.New(t)
	base := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	next := NextDay(base)
	r.Equal(base.Add(24*time.Hour), next)
}

// TestLastDay verifies that LastDay returns a time exactly 24 hours earlier.
func TestLastDay(t *testing.T) {
	r := require.New(t)
	base := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	last := LastDay(base)
	r.Equal(base.Add(-24*time.Hour), last)
}

// TestTimeStampToString verifies that a known Unix timestamp formats correctly.
func TestTimeStampToString(t *testing.T) {
	r := require.New(t)
	// 2024-01-15 08:30:00 UTC
	ts := time.Date(2024, 1, 15, 8, 30, 0, 0, time.UTC).Unix()
	// time.DateTime = "2006-01-02 15:04:05"
	got := TimeStampToString(ts)
	want := time.Unix(ts, 0).Format("2006-01-02 15:04:05")
	r.Equal(want, got)
}

// TestSecondToTime_Extended covers additional SecondToTime cases not
// covered by the existing TestSecondToTime.
func TestSecondToTime_Extended(t *testing.T) {
	r := require.New(t)

	// Zero seconds
	h, m, s := SecondToTime(0)
	r.EqualValues(0, h)
	r.EqualValues(0, m)
	r.EqualValues(0, s)

	// Exactly one hour
	h, m, s = SecondToTime(3600)
	r.EqualValues(1, h)
	r.EqualValues(0, m)
	r.EqualValues(0, s)

	// Exactly one minute
	h, m, s = SecondToTime(60)
	r.EqualValues(0, h)
	r.EqualValues(1, m)
	r.EqualValues(0, s)
}

// TestDayPass verifies that DayPass counts calendar days correctly.
func TestDayPass(t *testing.T) {
	r := require.New(t)

	loc := time.UTC

	// Same day: 0 days apart.
	begin := time.Date(2024, 5, 1, 8, 0, 0, 0, loc)
	end := time.Date(2024, 5, 1, 23, 59, 59, 0, loc)
	r.EqualValues(0, DayPass(begin, end))

	// Exactly one calendar day apart (midnight boundary).
	begin = time.Date(2024, 5, 1, 0, 0, 0, 0, loc)
	end = time.Date(2024, 5, 2, 0, 0, 0, 0, loc)
	r.EqualValues(1, DayPass(begin, end))

	// Spanning a month boundary.
	begin = time.Date(2024, 1, 30, 0, 0, 0, 0, loc)
	end = time.Date(2024, 2, 3, 0, 0, 0, 0, loc)
	r.EqualValues(4, DayPass(begin, end))

	// Negative when end is before begin.
	begin = time.Date(2024, 5, 5, 0, 0, 0, 0, loc)
	end = time.Date(2024, 5, 3, 0, 0, 0, 0, loc)
	r.EqualValues(-2, DayPass(begin, end))
}
