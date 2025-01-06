package task_group

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTaskPool(t *testing.T) {
	tp := NewTaskPool(1024, 100000)
	count := int64(0)
	for i := 0; i < 1024; i++ {
		go func() {
			for {
				tp.PutForce(
					func() {
						atomic.AddInt64(&count, 1)
					},
				)
			}
		}()
	}
	oldCount := int64(0)
	for {
		time.Sleep(time.Second)
		count := atomic.LoadInt64(&count)
		fmt.Println(count - oldCount)
		oldCount = count
	}
}
