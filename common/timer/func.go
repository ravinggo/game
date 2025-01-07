package timer

import (
	"time"
)

// AfterFunc 过多久后执行
func AfterFunc(duration time.Duration, f func()) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.AfterFunc(duration, f)
}

// UntilFunc 到时间点执行
func UntilFunc(t time.Time, f func()) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.UtilFunc(t, f)
}

// Ticker 间隔时间循环执行
func Ticker(duration time.Duration, f func() bool) {
	if f == nil {
		return
	}
	tk := newTicker()
	tk.TickFunc(duration, f)
}

func DayPass(begin time.Time, end time.Time) int64 {
	b := BeginOfDay(begin)
	e := BeginOfDay(end)
	d := int64(time.Hour * 24)
	return (e.UnixNano() - b.UnixNano()) / d
}

// BeginOfDay 返回一天的开始时刻
func BeginOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}

// EndOfDay 返回一天的结束时刻
func EndOfDay(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 23, 59, 59, 999999999, t.Location())
}

// NextDay 返回下一天的同一时刻
func NextDay(t time.Time) time.Time {
	d := time.Hour * 24
	return t.Add(d)
}

// LastDay  返回前一天的同一时刻
func LastDay(t time.Time) time.Time {
	d := time.Hour * -24
	return t.Add(d)
}

// BeginOfWeek 返回一周的开始时刻,默认为周日0点
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

// BeginOfMouth 返回月份的开始时间
func BeginOfMouth(t time.Time) time.Time {
	y, m, _ := t.Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, t.Location())
}

// EndOfWeek 返回一周的结束时刻
func EndOfWeek(t time.Time) time.Time {
	wd := t.Weekday()
	day := 7 - wd
	duration := time.Hour * time.Duration(day) * 24
	w := EndOfDay(t)
	w = w.Add(duration)
	return w
}

func GetRefreshTime(lastTime time.Time, refreshHour int, refreshMin int) (time.Time, time.Time) {
	y, m, d := lastTime.UTC().Date()
	todayRefreshTime := time.Date(y, m, d, refreshHour, refreshMin, 0, 0, lastTime.UTC().Location())
	tomorrowRefreshTime := time.Date(y, m, d+1, refreshHour, refreshMin, 0, 0, lastTime.UTC().Location())
	return todayRefreshTime, tomorrowRefreshTime
}

func GetNextRefreshTime(refreshHour int, refreshMin int) int64 {
	localNow := time.Now().UTC()
	todayRefreshTime, tomorrowRefreshTime := GetRefreshTime(localNow, refreshHour, refreshMin)
	if localNow.UTC().Unix() < todayRefreshTime.UTC().Unix() {
		return todayRefreshTime.UTC().Unix()
	}
	return tomorrowRefreshTime.UTC().Unix()
}

// OverRefreshTime 判断是否超过刷新时间
func OverRefreshTime(lastRefreshTimeSecond int64, refreshHour int, refreshMin int) bool {
	lastTime := time.Unix(lastRefreshTimeSecond, 0).UTC()
	todayRefreshTime, tomorrowRefreshTime := GetRefreshTime(lastTime, refreshHour, refreshMin)
	if lastTime.UTC().Unix() < todayRefreshTime.UTC().Unix() {
		return time.Now().Unix() > todayRefreshTime.UTC().Unix()
	}
	return time.Now().Unix() > tomorrowRefreshTime.UTC().Unix()
}

func TimeStampToString(nTimer int64) string {
	return time.Unix(nTimer, 0).Format(time.DateTime)
}

func SecondToTime(second int64) (h int64, m int64, s int64) {
	h = second / 3600
	second = second % 3600
	m = second / 60
	s = second % 60
	return
}

func MillisecondToTime(millisecond int64) (h int64, m int64, s int64) {
	return SecondToTime(millisecond / 1000)
}
