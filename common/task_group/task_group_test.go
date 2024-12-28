package task_group

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskGroup_Put(t *testing.T) {

	go func() {
		http.ListenAndServe(":7766", nil)
	}()
	count := int64(0)
	for i := 0; i < 5; i++ {
		go func() {
			tg := NewTaskGroup[func()](
				func(elem TaskGroupElem[func()]) {
					elem.Data()
				}, 128,
			)
			for {
				tg.Put(
					func() {
						atomic.AddInt64(&count, 1)
					}, nil,
				)
			}
		}()
	}
	old := int64(0)
	for {
		time.Sleep(time.Second)
		c := atomic.LoadInt64(&count)
		fmt.Println(c, c-old)
		old = c
	}
}
