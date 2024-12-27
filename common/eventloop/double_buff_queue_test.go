package eventloop

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestDoubleBuffQueue(t *testing.T) {

	el := NewDoubleBuffQueue(true)

	for i := 0; i < 3; i++ {
		go func() {
			for {
				el.PostEventQueue(
					1,
				)
			}
		}()
	}
	var count = int64(0)
	el.Start(
		func(event any) {
			count += int64(event.(int))
		},
	)

	oldCount := int64(0)
	for {
		newCount := atomic.LoadInt64(&count)
		fmt.Println(newCount, newCount-oldCount)
		oldCount = newCount
		time.Sleep(time.Second)
	}
}
