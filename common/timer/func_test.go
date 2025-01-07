package timer

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
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

func TestTicker(t *testing.T) {
	go func() {
		http.ListenAndServe(":8222", nil)
	}()
	count := int64(0)

	for i := 0; i < 100; i++ {
		go func() {
			for i := 0; i < 10000; i++ {
				interval := time.Second
				Ticker(
					interval, func() bool {
						atomic.AddInt64(&count, 1)
						return true
					},
				)
				time.Sleep(time.Microsecond)
			}
		}()
	}

	oldCount := int64(0)
	for {
		time.Sleep(time.Second)
		currCount := atomic.LoadInt64(&count)
		fmt.Println(currCount, currCount-oldCount)
		oldCount = currCount
	}
}
