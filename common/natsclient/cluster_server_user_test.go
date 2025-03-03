package natsclient

import (
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/logger"
)

func TestNewClusterClientServerUser(t *testing.T) {
	wg := sync.WaitGroup{}
	testData := &basepb.IntTrace{
		RoleId:         1,
		FromServerId:   2,
		FromServerType: "test",
		TraceId:        "xasdasda",
	}
	cuc := NewClusterClientServerUser[ServerIntUserSubject](
		[]string{"nats://127.0.0.1:4224"}, func(msg *nats.Msg) {
			defer wg.Done()
			index := strings.IndexByte(msg.Subject, '.')
			if index == -1 {
				panic("msg subject error")
			}
			msgName := msg.Subject[index+1:]
			typ, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(msgName))
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
	wg.Add(2)
	cuc.UserSubscribeOneWaitSuccess(
		&ServerIntUserSubject{
			ServerType: "1",
			ServerId:   2,
			RoleId:     1,
		},
	)
	err := cuc.PublishUser(
		nil,
		&ServerIntUserSubject{
			ServerType: "1",
			ServerId:   2,
			RoleId:     1,
		}, testData,
	)
	if err != nil {
		t.Fatal(err)
	}
	err = cuc.RequestUser(
		nil,
		&ServerIntUserSubject{
			ServerType: "1",
			ServerId:   2,
			RoleId:     1,
		}, testData, &basepb.IntTrace{},
	)
	if err != nil {
		t.Fatal(err)
	}
	cuc.UserUnsubscribe(
		&ServerIntUserSubject{
			ServerType: "1",
			ServerId:   2,
			RoleId:     1,
		},
	)
	wg.Wait()
}
