package ctx

import (
	"testing"

	"github.com/ravinggo/game/common/basepb"
)

type (
	objectPoolKey struct{}
	testKey       struct{}
)

func BenchmarkGetValue(b *testing.B) {
	var c Int64TraceCtx
	c.SetValue(objectPoolKey{}, 1)
	for i := 0; i < b.N; i++ {
		Value[objectPoolKey, int](&c, objectPoolKey{})
	}
}

// ---- IntTrace.GetRoleID ----

func TestIntTrace_GetRoleID_Positive(t *testing.T) {
	tr := &IntTrace{}
	tr.RoleId = 42
	if got := tr.GetRoleID(); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestIntTrace_GetRoleID_Negative(t *testing.T) {
	tr := &IntTrace{}
	tr.RoleId = -99
	if got := tr.GetRoleID(); got != -99 {
		t.Fatalf("expected -99, got %d", got)
	}
}

func TestIntTrace_GetRoleID_Zero(t *testing.T) {
	tr := &IntTrace{}
	if got := tr.GetRoleID(); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

// ---- IntTrace marshal roundtrip ----

func TestIntTrace_MarshalRoundtrip(t *testing.T) {
	orig := &IntTrace{}
	orig.RoleId = 123
	orig.FromServerId = 7
	orig.FromServerType = "gate"

	size := orig.TraceMarshalSize()
	if size <= 0 {
		t.Fatalf("expected positive size, got %d", size)
	}

	buf := make([]byte, 0, size)
	var err error
	buf, err = orig.TraceMarshalAppend(buf)
	if err != nil {
		t.Fatalf("TraceMarshalAppend: %v", err)
	}

	decoded := &IntTrace{}
	if err := decoded.TraceMarshalFrom(buf); err != nil {
		t.Fatalf("TraceMarshalFrom: %v", err)
	}

	if decoded.RoleId != orig.RoleId {
		t.Errorf("RoleId mismatch: want %d got %d", orig.RoleId, decoded.RoleId)
	}
	if decoded.FromServerId != orig.FromServerId {
		t.Errorf("FromServerId mismatch: want %d got %d", orig.FromServerId, decoded.FromServerId)
	}
	if decoded.FromServerType != orig.FromServerType {
		t.Errorf("FromServerType mismatch: want %s got %s", orig.FromServerType, decoded.FromServerType)
	}
	if decoded.TraceId == "" {
		t.Error("expected non-empty TraceId after marshal roundtrip")
	}
}

// ---- BaseCtx.Reset ----

func TestBaseCtx_Reset_ClearsState(t *testing.T) {
	var c Int64TraceCtx
	c.SetValue(testKey{}, "hello")
	c.NatsMsg = nil
	c.Resp = &basepb.IntTrace{}
	c.OtherResp = append(c.OtherResp, &basepb.IntTrace{})
	c.TD.RoleId = 55

	c.Reset()

	if c.Resp != nil {
		t.Error("Resp should be nil after Reset")
	}
	if len(c.OtherResp) != 0 {
		t.Errorf("OtherResp should be empty after Reset, got len=%d", len(c.OtherResp))
	}
	if c.NatsMsg != nil {
		t.Error("NatsMsg should be nil after Reset")
	}
	if c.TD.RoleId != 0 {
		t.Errorf("TD.RoleId should be 0 after Reset, got %d", c.TD.RoleId)
	}
	if c.Context == nil {
		t.Error("Context should not be nil after Reset")
	}
}

// ---- BaseCtx.SetValue / Value ----

func TestBaseCtx_SetValue_Value(t *testing.T) {
	var c Int64TraceCtx
	c.SetValue(testKey{}, "world")
	got, ok := Value[testKey, string](&c, testKey{})
	if !ok {
		t.Fatal("expected Value to return ok=true")
	}
	if got != "world" {
		t.Errorf("expected %q, got %q", "world", got)
	}
}

func TestBaseCtx_Value_Miss(t *testing.T) {
	var c Int64TraceCtx
	c.Reset()
	_, ok := Value[testKey, string](&c, testKey{})
	if ok {
		t.Fatal("expected ok=false for missing key")
	}
}

func TestBaseCtx_Value_WrongType(t *testing.T) {
	var c Int64TraceCtx
	c.SetValue(testKey{}, 42)
	_, ok := Value[testKey, string](&c, testKey{})
	if ok {
		t.Fatal("expected ok=false when stored type does not match requested type")
	}
}

// ---- BaseCtx.GetTrace ----

func TestBaseCtx_GetTrace_NonNil(t *testing.T) {
	var c Int64TraceCtx
	tr := c.GetTrace()
	if tr == nil {
		t.Fatal("GetTrace should never return nil")
	}
}

// ---- IntTrace.GetServerIdAndType / SetServerIdAndType ----

func TestIntTrace_ServerIdAndType(t *testing.T) {
	tr := &IntTrace{}
	tr.SetServerIdAndType(99, "gate")
	id, typ := tr.GetServerIdAndType()
	if id != 99 || typ != "gate" {
		t.Errorf("expected (99, gate), got (%d, %s)", id, typ)
	}
}

// ---- IntTrace.TraceMarshalSize is stable across repeated calls ----

func TestIntTrace_TraceMarshalSize_Stable(t *testing.T) {
	tr := &IntTrace{}
	tr.RoleId = 1
	s1 := tr.TraceMarshalSize()
	s2 := tr.TraceMarshalSize()
	if s1 != s2 {
		t.Errorf("TraceMarshalSize should be stable: %d vs %d", s1, s2)
	}
}

// ---- BaseCtx logging methods ----

func TestBaseCtx_LogMethods_NoPanic(t *testing.T) {
	var c Int64TraceCtx
	c.Reset()
	c.TD.RoleId = 1
	_ = c.Trace()
	_ = c.Debug()
	_ = c.Info()
	_ = c.Warn()
	_ = c.Error()
	_ = c.NoLevel()
	_ = c.Disabled()
}

func TestBaseCtx_Disabled_ReturnsNil(t *testing.T) {
	var c Int64TraceCtx
	if c.Disabled() != nil {
		t.Error("Disabled() should always return nil")
	}
}

func TestBaseCtx_WithLevel_NoPanic(t *testing.T) {
	var c Int64TraceCtx
	c.Reset()
	_ = c.WithLevel(1)
}

// ---- BaseCtx nil-inner-context paths ----

func TestBaseCtx_Deadline_NilContext(t *testing.T) {
	var c Int64TraceCtx
	dl, ok := c.Deadline()
	if ok || !dl.IsZero() {
		t.Error("Deadline on nil context should return zero time and false")
	}
}

func TestBaseCtx_Done_NilContext(t *testing.T) {
	var c Int64TraceCtx
	if c.Done() != nil {
		t.Error("Done on nil context should return nil")
	}
}

func TestBaseCtx_Err_NilContext(t *testing.T) {
	var c Int64TraceCtx
	if c.Err() != nil {
		t.Error("Err on nil context should return nil")
	}
}

func TestBaseCtx_Value_NilContext(t *testing.T) {
	var c Int64TraceCtx
	if c.Value(testKey{}) != nil {
		t.Error("Value on nil context should return nil")
	}
}

// ---- SetValue creates background context when nil ----

func TestBaseCtx_SetValue_CreatesContextWhenNil(t *testing.T) {
	var c Int64TraceCtx
	c.SetValue(testKey{}, "auto-bg")
	if c.Context == nil {
		t.Fatal("SetValue should have created a background context")
	}
	got, ok := Value[testKey, string](&c, testKey{})
	if !ok || got != "auto-bg" {
		t.Errorf("expected 'auto-bg', ok=true; got %q, ok=%v", got, ok)
	}
}

// ---- IntTrace.TraceMarshalFrom ----

func TestIntTrace_TraceMarshalFrom_EmptyTraceId(t *testing.T) {
	bare := &IntTrace{}
	bare.RoleId = 7
	bareSz := bare.TraceMarshalSize()
	bareBuf := make([]byte, 0, bareSz)
	bareBuf, _ = bare.TraceMarshalAppend(bareBuf)

	decoded := &IntTrace{}
	if err := decoded.TraceMarshalFrom(bareBuf); err != nil {
		t.Fatalf("TraceMarshalFrom: %v", err)
	}
	if decoded.RoleId != 7 {
		t.Errorf("expected RoleId 7, got %d", decoded.RoleId)
	}
}

// ---- compile-time interface checks ----

var (
	_ Trace    = (*IntTrace)(nil)
	_ IContext = (*Int64TraceCtx)(nil)
)
