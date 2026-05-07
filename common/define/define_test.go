// Package define_test contains tests for the proto serialisation helpers and shared
// type definitions in the define package.
// Written by Claude Code claude-opus-4-6.
package define_test

import (
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ravinggo/game/common/basepb"
	"github.com/ravinggo/game/common/define"
)

// ---- ProtoMessageName ----

func TestProtoMessageName_NonEmpty(t *testing.T) {
	msg := &basepb.IntTrace{}
	name := define.ProtoMessageName(msg)
	if name == "" {
		t.Fatal("expected non-empty message name")
	}
	// gogo-protobuf types expose XXX_MessageName, so we expect the full name.
	if name != "basepb.IntTrace" {
		t.Errorf("unexpected message name: %q", name)
	}
}

// ---- ProtoSize ----

func TestProtoSize_PositiveForNonEmpty(t *testing.T) {
	msg := &basepb.IntTrace{RoleId: 42, FromServerType: "gate"}
	size := define.ProtoSize(msg)
	if size <= 0 {
		t.Fatalf("expected positive size for non-empty message, got %d", size)
	}
}

func TestProtoSize_ZeroForEmpty(t *testing.T) {
	msg := &basepb.IntTrace{}
	size := define.ProtoSize(msg)
	// An all-zero proto3 message has zero encoded size.
	if size != 0 {
		t.Fatalf("expected size 0 for empty message, got %d", size)
	}
}

// ---- ProtoUnmarshal roundtrip (via ProtoMarshalAppend) ----

func TestProtoUnmarshal_Roundtrip_IntTrace(t *testing.T) {
	orig := &basepb.IntTrace{
		RoleId:         999,
		FromServerId:   7,
		FromServerType: "logic",
		TraceId:        "trace-abc",
	}

	size := define.ProtoSize(orig)
	buf := make([]byte, 0, size)
	var err error
	buf, err = define.ProtoMarshalAppend(buf, orig)
	if err != nil {
		t.Fatalf("ProtoMarshalAppend: %v", err)
	}
	if len(buf) == 0 {
		t.Fatal("marshalled bytes should not be empty")
	}

	decoded := &basepb.IntTrace{}
	if err := define.ProtoUnmarshal(buf, decoded); err != nil {
		t.Fatalf("ProtoUnmarshal: %v", err)
	}

	if decoded.RoleId != orig.RoleId {
		t.Errorf("RoleId: want %d got %d", orig.RoleId, decoded.RoleId)
	}
	if decoded.FromServerId != orig.FromServerId {
		t.Errorf("FromServerId: want %d got %d", orig.FromServerId, decoded.FromServerId)
	}
	if decoded.FromServerType != orig.FromServerType {
		t.Errorf("FromServerType: want %q got %q", orig.FromServerType, decoded.FromServerType)
	}
	if decoded.TraceId != orig.TraceId {
		t.Errorf("TraceId: want %q got %q", orig.TraceId, decoded.TraceId)
	}
}

// ---- ProtoMarshalAppend appends, does not overwrite ----

func TestProtoMarshalAppend_AppendsToExistingSlice(t *testing.T) {
	msg := &basepb.IntTrace{RoleId: 1}
	msgSize := define.ProtoSize(msg)

	// Allocate a buffer with the prefix at the front and enough total capacity for
	// the prefix + the encoded message. The gogo MarshalTo path writes into
	// b[len(b):] and requires that slice to have sufficient capacity.
	buf := make([]byte, 2, 2+msgSize)
	buf[0] = 0xDE
	buf[1] = 0xAD

	result, err := define.ProtoMarshalAppend(buf, msg)
	if err != nil {
		t.Fatalf("ProtoMarshalAppend: %v", err)
	}
	if len(result) <= 2 {
		t.Fatalf("expected appended bytes, got len=%d", len(result))
	}
	if result[0] != 0xDE || result[1] != 0xAD {
		t.Error("ProtoMarshalAppend should not overwrite the existing prefix")
	}
}

// ---- Standard proto (non-gogo) fallback paths ----
// timestamppb.Timestamp does not implement MarshalAppender or gogo Unmarshaler, so
// these tests exercise the proto.MarshalOptions / proto.Unmarshal fallback branches.

func TestProtoSize_StandardProto(t *testing.T) {
	msg := timestamppb.Now()
	size := define.ProtoSize(msg)
	if size <= 0 {
		t.Fatalf("expected positive size for timestamppb.Now(), got %d", size)
	}
}

func TestProtoMarshalAppend_StandardProto(t *testing.T) {
	msg := timestamppb.Now()
	buf, err := define.ProtoMarshalAppend(nil, msg)
	if err != nil {
		t.Fatalf("ProtoMarshalAppend (standard proto): %v", err)
	}
	if len(buf) == 0 {
		t.Fatal("expected non-empty buffer for standard proto message")
	}
}

func TestProtoUnmarshal_StandardProto(t *testing.T) {
	orig := timestamppb.Now()
	buf, err := define.ProtoMarshalAppend(nil, orig)
	if err != nil {
		t.Fatalf("ProtoMarshalAppend: %v", err)
	}

	decoded := &timestamppb.Timestamp{}
	if err := define.ProtoUnmarshal(buf, decoded); err != nil {
		t.Fatalf("ProtoUnmarshal (standard proto): %v", err)
	}
	if decoded.Seconds != orig.Seconds || decoded.Nanos != orig.Nanos {
		t.Errorf("roundtrip mismatch: want %v got %v", orig, decoded)
	}
}

func TestProtoMessageName_StandardProto(t *testing.T) {
	msg := &timestamppb.Timestamp{}
	name := define.ProtoMessageName(msg)
	if name == "" {
		t.Fatal("expected non-empty message name for standard proto type")
	}
}

// ---- Sentinel error values ----

func TestSentinelErrors_NotNil(t *testing.T) {
	if define.ErrInvalidUserSubj == nil {
		t.Error("ErrInvalidUserSubj should not be nil")
	}
	if define.ErrZeroRoleID == nil {
		t.Error("ErrZeroRoleID should not be nil")
	}
}

func TestSentinelErrors_Distinct(t *testing.T) {
	if define.ErrInvalidUserSubj == define.ErrZeroRoleID {
		t.Error("sentinel errors should be distinct values")
	}
}
