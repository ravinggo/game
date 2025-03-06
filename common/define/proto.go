package define

import (
	gogo "github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type MarshalAppender interface {
	MarshalTo(data []byte) (n int, err error)
}

type XXXMessageNamer interface {
	XXX_MessageName() string
}

func ProtoSize(msg proto.Message) int {
	if sizer, ok := msg.(gogo.Sizer); ok {
		return sizer.Size()
	}

	return proto.Size(msg)
}

func ProtoMarshalAppend(b []byte, msg proto.Message) ([]byte, error) {
	if ma, ok := msg.(MarshalAppender); ok {
		write := b[len(b):]
		n, err := ma.MarshalTo(write)
		if err != nil {
			return nil, err
		}
		return b[:len(b)+n], nil
	}

	return proto.MarshalOptions{}.MarshalAppend(b, msg)
}

// func ProtoMarshal(msg proto.Message) ([]byte, error) {
// 	if ma, ok := msg.(gogo.Marshaler); ok {
// 		write := b[len(b):]
// 		n, err := ma.MarshalTo(write)
// 		if err != nil {
// 			return nil, err
// 		}
// 		return b[:len(b)+n], nil
// 	}
//
// }

func ProtoUnmarshal(b []byte, msg proto.Message) error {
	if unmarshaler, ok := msg.(gogo.Unmarshaler); ok {
		return unmarshaler.Unmarshal(b)
	}

	return proto.Unmarshal(b, msg)
}

func ProtoMessageName(msg proto.Message) protoreflect.FullName {
	if namer, ok := msg.(XXXMessageNamer); ok {
		return protoreflect.FullName(namer.XXX_MessageName())
	}

	return proto.MessageName(msg)
}
