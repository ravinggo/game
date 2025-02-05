package natsclient

import (
	"errors"

	"github.com/nats-io/nats.go"
)

const (
	defaultQueueChanSize = 10240
)

type UNOption func(*UNOptions) error

type UNOptions struct {
	opts          []nats.Option
	ChanCount     int
	Queues        []chan *nats.Msg
	UserHandler   nats.MsgHandler
	QueueChanSize int // default defaultQueueChanSize
}

func NatsOptions(opts ...nats.Option) UNOption {
	return func(u *UNOptions) error {
		u.opts = append(u.opts, opts...)
		return nil
	}
}

// UserQueueChanCount set the queue chan count, and per chan size is defaultQueueChanSize.
func UserQueueChanCount(chanCount int, queueChanSize int) UNOption {
	return func(o *UNOptions) error {
		if len(o.Queues) != 0 {
			return errors.New("can not set ChanCount and Queues at the same time")
		}
		o.ChanCount = chanCount
		o.QueueChanSize = queueChanSize
		return nil
	}
}

func UserQueues(queues []chan *nats.Msg) UNOption {
	return func(o *UNOptions) error {
		if o.ChanCount != 0 {
			return errors.New("can not set ChanCount and Queues at the same time")
		}
		o.Queues = queues
		return nil
	}
}
