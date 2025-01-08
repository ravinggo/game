package define

import (
	"sync"

	"google.golang.org/protobuf/proto"
)

type ServerType string

func (s ServerType) String() string {
	return string(s)
}

type ProtoMessagePtr[T any] interface {
	proto.Message
	*T
}

type Ptr[T any] interface {
	*T
}

func GetProtoMessage[PB ProtoMessagePtr[T], T any](t *T) PB {
	return PB(t)
}

type Clear interface {
	Reset()
}

type DoNotCopy [0]sync.Mutex
