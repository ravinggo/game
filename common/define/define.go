package define

import (
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
