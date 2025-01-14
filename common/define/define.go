package define

import (
	"sync"

	"google.golang.org/protobuf/proto"
)

// ProtoMessagePtr is an interface that can be implemented by a struct to be a proto message pointer
// for enforce constraints on proto message pointer when using generics
type ProtoMessagePtr[T any] interface {
	proto.Message
	*T
}

// Clear is an interface that can be implemented by a struct to reset its value
// Just for the convenience of objectpool
type Clear interface {
	Reset()
}

// DoNotCopy is an empty structure that can be embedded into a struct to prevent
// It cannot be copied
type DoNotCopy [0]sync.Mutex
