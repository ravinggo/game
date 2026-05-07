package define

import (
	gogo "github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MarshalAppender is implemented by gogo-protobuf generated types that can serialise
// themselves directly into a pre-allocated byte slice, avoiding an extra allocation
// compared to the standard proto.Marshal path.
// Written by Claude Code claude-opus-4-6.
type MarshalAppender interface {
	MarshalTo(data []byte) (n int, err error)
}

// XXXMessageNamer is implemented by gogo-protobuf generated types that expose their
// fully-qualified message name without requiring protoreflect. The framework uses this
// to determine NATS subjects at handler registration and dispatch time.
// Written by Claude Code claude-opus-4-6.
type XXXMessageNamer interface {
	XXX_MessageName() string
}

// ProtoSize returns the number of bytes required to serialise msg. It prefers the
// gogo-protobuf Sizer interface (zero-allocation) when available, falling back to the
// standard google.golang.org/protobuf proto.Size otherwise.
// Written by Claude Code claude-opus-4-6.
func ProtoSize(msg proto.Message) int {
	if sizer, ok := msg.(gogo.Sizer); ok {
		return sizer.Size()
	}

	return proto.Size(msg)
}

// ProtoMarshalAppend serialises msg and appends the bytes to b, returning the extended
// slice. If msg implements MarshalAppender (gogo-protobuf) the bytes are written directly
// into the existing buffer capacity without allocation; otherwise the standard
// proto.MarshalOptions.MarshalAppend path is used.
// Written by Claude Code claude-opus-4-6.
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

// ProtoUnmarshal deserialises b into msg. If msg implements the gogo-protobuf Unmarshaler
// interface it is used directly; otherwise the standard google.golang.org/protobuf
// proto.Unmarshal is called.
// Written by Claude Code claude-opus-4-6.
func ProtoUnmarshal(b []byte, msg proto.Message) error {
	if unmarshaler, ok := msg.(gogo.Unmarshaler); ok {
		return unmarshaler.Unmarshal(b)
	}

	return proto.Unmarshal(b, msg)
}

// ProtoMessageName returns the fully-qualified protobuf message name for msg. It prefers
// the XXXMessageNamer interface (gogo-protobuf) when available; otherwise it delegates to
// proto.MessageName from the google.golang.org/protobuf reflection API.
// Written by Claude Code claude-opus-4-6.
func ProtoMessageName(msg proto.Message) protoreflect.FullName {
	if namer, ok := msg.(XXXMessageNamer); ok {
		return protoreflect.FullName(namer.XXX_MessageName())
	}

	return proto.MessageName(msg)
}
