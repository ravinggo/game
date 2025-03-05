package natsclient

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/define"
	"github.com/ravinggo/game/common/logger"
)

func TestNatsClient(t *testing.T) {
	nc := NewNatsClient("nats://127.0.0.1:4224", time.Second*10, nats.Name("test"))
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
			err = define.ProtoUnmarshal(msg.Data[2:], m)
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
		nc.Unsubscribe("basepb.>")
	}()
	wg.Wait()
	nc.QueueSubscribeWaitSuccess(
		"basepb.>", func(msg *nats.Msg) {
			defer wg.Done()
			typ, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(msg.Subject))
			if err != nil {
				panic(err)
			}
			m := typ.New().Interface()
			err = define.ProtoUnmarshal(msg.Data[2:], m)
			if err != nil {
				panic(err)
			}
			intTrace := m.(*basepb.IntTrace)
			if intTrace.RoleId != testData.RoleId {
				t.Fatalf("RoleId != %d, want %d", intTrace.RoleId, testData.RoleId)
			}
			if intTrace.FromServerId != testData.FromServerId {
				t.Fatalf("FromServerId != %d, want %d", intTrace.FromServerId, testData.FromServerId)
			}
			if intTrace.FromServerType != testData.FromServerType {
				t.Fatalf("FromServerType != %s, want %s", intTrace.FromServerType, testData.FromServerType)
			}
			if intTrace.TraceId != testData.TraceId {
				t.Fatalf("TraceId!= %s, want %s", intTrace.TraceId, testData.TraceId)
			}
			if msg.Reply != "" {
				e := natsMsgReplyOne(msg, intTrace)
				if e != nil {
					t.Fatalf("natsMsgReplyOne err:%v", e)
				}
			}
			logger.Log.Info().Msg("nats client test success")
		},
	)
	wg.Add(2)
	go func() {
		err := nc.Publish(
			nil, testData,
		)
		if err != nil {
			panic(fmt.Sprintf("NatsClient Publish err:%v", err))
		}

		err = nc.Request(nil, testData, &basepb.IntTrace{})
		if err != nil {
			panic(fmt.Sprintf("NatsClient Request err:%v", err))
		}
		nc.Unsubscribe("basepb.>")
	}()
	wg.Wait()

	nc.Close()
}

func TestClusterClient(t *testing.T) {
	cnc := NewClusterClient([]string{"nats://127.0.0.1:4224"}, time.Second*10, nats.Name("test"))
	wg := &sync.WaitGroup{}
	testData := &basepb.IntTrace{
		RoleId:         1,
		FromServerId:   2,
		FromServerType: "test",
		TraceId:        "xasdasda",
	}
	cnc.QueueSubscribeAllWaitSuccess(
		"basepb.>", func(msg *nats.Msg) {

			defer wg.Done()
			typ, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(msg.Subject))
			if err != nil {
				panic(err)
			}
			m := typ.New().Interface()
			err = define.ProtoUnmarshal(msg.Data[2:], m)
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
			if msg.Reply != "" {
				e := natsMsgReplyOne(msg, intTrace)
				if e != nil {
					t.Fatalf("natsMsgReplyOne err:%v", e)
				}
			}
			logger.Log.Info().Msg("nats cluster client test success")
		},
	)
	wg.Add(1)
	go func() {
		err := cnc.Publish(
			nil, testData,
		)
		if err != nil {
			panic(err)
		}
	}()

	wg.Wait()
	cnc.Unsubscribe("basepb.>")
	cnc.Close()
}
