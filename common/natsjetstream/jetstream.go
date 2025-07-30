package natsjetstream

import (
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/ravinggo/game/common/berror"
	"github.com/ravinggo/game/common/logger"
)

type JetStream struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func NewJetStream(nc *nats.Conn) *JetStream {
	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}
	return &JetStream{nc: nc, js: js}
}

func (this_ *JetStream) Close() {
	_ = this_.nc.Drain()
}

func getSubjByStream(name string) string {
	return name + ".>"
}

func (this_ *JetStream) AddStream(name string, desc string, maxBytes int64) *berror.ErrMsg {
	info, err := this_.js.AddStream(
		&nats.StreamConfig{
			Name:        name,
			Description: desc,
			Subjects:    []string{name},
			Retention:   nats.WorkQueuePolicy,
			MaxBytes:    maxBytes,
		},
	)

	if err != nil {
		return berror.NewProtocolErr(err)
	}
	logger.Log.Info().Any("stream", info).Msg("add stream")
	return nil
}

func (this_ *JetStream) AddStreamIfNoExist(name string, desc string, maxBytes int64) *berror.ErrMsg {
	info, err := this_.js.StreamNameBySubject(name)
	if err != nil {
		if !errors.Is(err, nats.ErrNoMatchingStream) {
			return berror.NewProtocolErr(err)
		}
		return this_.AddStream(name, desc, maxBytes)
	}
	logger.Log.Info().Any("stream", info).Msg("stream exist")
	return nil
}

func (this_ *JetStream) DeleteStreamIfExist(name string) *berror.ErrMsg {
	subj := getSubjByStream(name)
	_, err := this_.js.StreamNameBySubject(subj)
	if err != nil {
		if !errors.Is(err, nats.ErrNoMatchingStream) {
			return berror.NewProtocolErr(err)
		}
		return nil
	}

	return berror.NewProtocolErr(this_.js.DeleteStream(name))
}

func (this_ *JetStream) Publish(subj string, data []byte) *berror.ErrMsg {
	_, err := this_.js.Publish(subj, data, nats.AckWait(time.Second*5))
	if err != nil {
		return berror.NewProtocolErr(err)
	}
	return nil
}

func (this_ *JetStream) JetStream() nats.JetStream {
	return this_.js
}

func (this_ *JetStream) ConsumerInfo(stream string, name string) (*nats.ConsumerInfo, *berror.ErrMsg) {
	ci, err := this_.js.ConsumerInfo(stream, name)
	if err != nil {
		return nil, berror.NewProtocolErr(err)
	}
	return ci, nil
}

func (this_ *JetStream) UpdateConsumer(stream string, name string, seq uint64) *berror.ErrMsg {
	ci, err := this_.ConsumerInfo(stream, name)
	if err != nil {
		return err
	}
	if ci.Config.DeliverPolicy != nats.DeliverByStartSequencePolicy {
		ci.Config.DeliverPolicy = nats.DeliverByStartSequencePolicy
		ci.Config.OptStartSeq = seq
		newCi, e := this_.js.UpdateConsumer(stream, &ci.Config)
		if e != nil {
			return berror.NewProtocolErr(err)
		}
		logger.Log.Info().Any("consumer", newCi).Str("stream", stream).Msg("update consumer")
	}
	return nil
}

func (this_ *JetStream) DeleteConsumer(stream string, name string) *berror.ErrMsg {
	ci, err := this_.ConsumerInfo(stream, name)
	if err != nil {
		return err
	}
	e := this_.js.DeleteConsumer(stream, ci.Name)
	return berror.NewProtocolErr(e)
}

// PullWithSeq Start pulling data from the specified Sequence
func (this_ *JetStream) PullWithSeq(stream string, name string, seq uint64) (*Puller, *berror.ErrMsg) {
	return this_.pullWith(getSubjByStream(stream), name, nats.StartSequence(seq), nats.BindStream(stream))
}

// PullWithTime Start pulling data from the specified time
func (this_ *JetStream) PullWithTime(stream string, name string, time time.Time) (*Puller, *berror.ErrMsg) {
	return this_.pullWith(getSubjByStream(stream), name, nats.StartTime(time), nats.BindStream(stream))
}

// PullWithAll Pull all data
func (this_ *JetStream) PullWithAll(stream string, name string) (*Puller, *berror.ErrMsg) {
	return this_.pullWith(getSubjByStream(stream), name, nats.DeliverAll(), nats.BindStream(stream))
}

// PullWithNewest Pull the latest data
func (this_ *JetStream) PullWithNewest(stream string, name string) (*Puller, *berror.ErrMsg) {
	return this_.pullWith(getSubjByStream(stream), name, nats.DeliverNew(), nats.BindStream(stream))
}

func (this_ *JetStream) pullWith(subj string, name string, opt ...nats.SubOpt) (*Puller, *berror.ErrMsg) {
	s, err := this_.js.PullSubscribe(subj, name, opt...)
	if err != nil {
		return nil, berror.NewProtocolErr(err)
	}

	return &Puller{s: s}, nil
}

type Puller struct {
	s *nats.Subscription
}

func (this_ *Puller) Fetch(max int, maxWait time.Duration) ([]*nats.Msg, *berror.ErrMsg) {
	msgS, err := this_.s.Fetch(max, nats.MaxWait(maxWait))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		return nil, berror.NewProtocolErr(err)
	}
	return msgS, nil
}

func (this_ *Puller) Close() {
	_ = this_.s.Drain()
}
