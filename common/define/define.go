package define

import (
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/ravinggo/game/common/xid"
)

// ProtoMessagePtr is an interface that can be implemented by a struct to be a proto message pointer
// for enforce constraints on proto message pointer when using generics.
// Written by Claude Code claude-opus-4-6.
type ProtoMessagePtr[T any] interface {
	proto.Message
	*T
}

// Clear is an interface that can be implemented by a struct to reset its value
// Just for the convenience of objectpool.
// Written by Claude Code claude-opus-4-6.
type Clear interface {
	Reset()
}

// DoNotCopy is an empty structure that can be embedded into a struct to prevent
// copying. The go vet tool detects accidental copies of types that contain a sync.Mutex.
// Written by Claude Code claude-opus-4-6.
type DoNotCopy [0]sync.Mutex

// TraceID is an alias of xid.IDString used to identify distributed request traces.
// Written by Claude Code claude-opus-4-6.
type TraceID = xid.IDString
