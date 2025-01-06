package natsclient

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/logger"
)

func TestNatsClient(t *testing.T) {
	nc := NewNatsClient("test", "nats://127.0.0.1:4224", time.Second*10)
	wg := &sync.WaitGroup{}
	testData := &basepb.IntTrace{
		RoleId:         1,
		FromServerId:   2,
		FromServerType: "test",
		TraceId:        "xasdasda",
	}
	nc.QueueSubscribe(
		"basepb.>", func(msg *nats.Msg) {
			defer wg.Done()
			typ, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(msg.Subject))
			if err != nil {
				panic(err)
			}
			m := typ.New().Interface()
			err = proto.Unmarshal(msg.Data[2:], m)
			if err != nil {
				panic(err)
			}
			intTrace := m.(*basepb.IntTrace)
			if intTrace.RoleId != testData.RoleId {
				t.Errorf("RoleId != %d, want %d", intTrace.RoleId, testData.RoleId)
			}
			if intTrace.FromServerId != testData.FromServerId {
				t.Errorf("FromServerId != %d, want %d", intTrace.FromServerId, testData.FromServerId)
			}
			if intTrace.FromServerType != testData.FromServerType {
				t.Errorf("FromServerType != %s, want %s", intTrace.FromServerType, testData.FromServerType)
			}
			if intTrace.TraceId != testData.TraceId {
				t.Errorf("TraceId!= %s, want %s", intTrace.TraceId, testData.TraceId)
			}
			logger.Log.Info().Msg("nats client test success")
		},
	)
	wg.Add(1)
	go func() {
		err := nc.Publish(
			nil, testData,
		)
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()
}
