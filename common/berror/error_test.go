package berror

import (
	"errors"
	"testing"

	baseenv "github.com/ravinggo/game/common/base-env"
)

func TestStackTrace(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = true
	e := NewDatabaseStr("xxxx")
	if len(e.StackStace) == 0 {
		t.Error("StackTrace not set")
	}
	t.Log(e.String())
}

func BenchmarkErrorString(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	err := NewProtocolStr("xxxxx")
	for i := 0; i < b.N; i++ {
		err.String()
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewNormalStr(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNormalStr("user_error", "detail info")
	if e == nil {
		t.Fatal("expected non-nil ErrMsg")
	}
	if e.ErrMsg != "user_error" {
		t.Errorf("expected ErrMsg='user_error', got %q", e.ErrMsg)
	}
	if e.ErrInternalInfo != "detail info" {
		t.Errorf("expected ErrInternalInfo='detail info', got %q", e.ErrInternalInfo)
	}
}

func TestNewNormalErr_Nil(t *testing.T) {
	e := NewNormalErr("msg", nil)
	if e != nil {
		t.Fatal("expected nil for nil error input")
	}
}

func TestNewNormalErr_PlainError(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNormalErr("wrapper", errors.New("underlying"))
	if e == nil {
		t.Fatal("expected non-nil ErrMsg")
	}
	if !e.IsErrorNormal() {
		t.Error("expected IsErrorNormal() to be true")
	}
}

func TestNewNormalErr_AlreadyErrMsg(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	original := NewNormalStr("orig", "orig detail")
	wrapped := NewNormalErr("other", original)
	if wrapped != original {
		t.Error("expected the original ErrMsg to be returned unchanged")
	}
}

func TestNewProtocolStr(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewProtocolStr("proto detail")
	if e == nil {
		t.Fatal("expected non-nil ErrMsg")
	}
	if !e.IsErrorProtocol() {
		t.Error("expected IsErrorProtocol() to be true")
	}
	if e.ErrMsg != ServerInternalErrorStr {
		t.Errorf("expected ErrMsg=%q, got %q", ServerInternalErrorStr, e.ErrMsg)
	}
}

func TestNewProtocolErr_Nil(t *testing.T) {
	if NewProtocolErr(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestNewProtocolErr_PlainError(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewProtocolErr(errors.New("bad proto"))
	if e == nil || !e.IsErrorProtocol() {
		t.Error("expected ETProtocol ErrMsg")
	}
}

func TestNewPanicStr(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewPanicStr("panic detail")
	if e == nil {
		t.Fatal("expected non-nil ErrMsg")
	}
	if !e.IsErrorPanic() {
		t.Error("expected IsErrorPanic() to be true")
	}
}

func TestNewPanicErr_Nil(t *testing.T) {
	if NewPanicErr(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestNewPanicErr_PlainError(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewPanicErr(errors.New("oops"))
	if e == nil || !e.IsErrorPanic() {
		t.Error("expected ETPanic ErrMsg")
	}
}

func TestNewDatabaseStr(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewDatabaseStr("db detail")
	if e == nil {
		t.Fatal("expected non-nil ErrMsg")
	}
	if !e.IsErrorDatabase() {
		t.Error("expected IsErrorDatabase() to be true")
	}
}

func TestNewDatabaseErr_Nil(t *testing.T) {
	if NewDatabaseErr(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestNewDatabaseErr_PlainError(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewDatabaseErr(errors.New("db error"))
	if e == nil || !e.IsErrorDatabase() {
		t.Error("expected ETDataBase ErrMsg")
	}
}

func TestNewNoAuthStr(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNoAuthStr("no auth detail")
	if e == nil {
		t.Fatal("expected non-nil ErrMsg")
	}
	if !e.IsErrorNoAuth() {
		t.Error("expected IsErrorNoAuth() to be true")
	}
}

func TestNewNoAuthErr_Nil(t *testing.T) {
	if NewNoAuthErr(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestNewNoAuthErr_PlainError(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNoAuthErr(errors.New("forbidden"))
	if e == nil || !e.IsErrorNoAuth() {
		t.Error("expected ETNoAuth ErrMsg")
	}
}

// ---------------------------------------------------------------------------
// Classification correctness
// ---------------------------------------------------------------------------

func TestClassification_MutuallyExclusive(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	cases := []struct {
		name string
		e    *ErrMsg
		want string
	}{
		{"normal", NewNormalStr("m", "d"), "normal"},
		{"protocol", NewProtocolStr("d"), "protocol"},
		{"panic", NewPanicStr("d"), "panic"},
		{"database", NewDatabaseStr("d"), "database"},
		{"noauth", NewNoAuthStr("d"), "noauth"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var active []string
			if tc.e.IsErrorNormal() {
				active = append(active, "normal")
			}
			if tc.e.IsErrorProtocol() {
				active = append(active, "protocol")
			}
			if tc.e.IsErrorPanic() {
				active = append(active, "panic")
			}
			if tc.e.IsErrorDatabase() {
				active = append(active, "database")
			}
			if tc.e.IsErrorNoAuth() {
				active = append(active, "noauth")
			}
			if len(active) != 1 {
				t.Errorf("expected exactly 1 active classification, got %v", active)
			}
			if len(active) == 1 && active[0] != tc.want {
				t.Errorf("expected %q active, got %q", tc.want, active[0])
			}
		})
	}
}

func TestClassification_NilReceiver(t *testing.T) {
	var e *ErrMsg
	if e.IsErrorNormal() {
		t.Error("nil.IsErrorNormal() should be false")
	}
	if e.IsErrorProtocol() {
		t.Error("nil.IsErrorProtocol() should be false")
	}
	if e.IsErrorPanic() {
		t.Error("nil.IsErrorPanic() should be false")
	}
	if e.IsErrorDatabase() {
		t.Error("nil.IsErrorDatabase() should be false")
	}
	if e.IsErrorNoAuth() {
		t.Error("nil.IsErrorNoAuth() should be false")
	}
}

// ---------------------------------------------------------------------------
// Error() / String() tests
// ---------------------------------------------------------------------------

func TestErrMsg_Error_NonEmpty(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewProtocolStr("test detail")
	s := e.Error()
	if s == "" {
		t.Fatal("Error() must return a non-empty string")
	}
}

func TestErrMsg_Error_Nil(t *testing.T) {
	var e *ErrMsg
	if e.Error() != "nil" {
		t.Fatalf("expected 'nil', got %q", e.Error())
	}
}

func TestErrMsg_String_MatchesError(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNormalStr("m", "d")
	if e.String() != e.Error() {
		t.Error("String() must return the same value as Error()")
	}
}

// ---------------------------------------------------------------------------
// WithStackTrace / StackTrace tests
// ---------------------------------------------------------------------------

func TestWithStackTrace(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNormalStr("m", "d")
	if len(e.StackStace) != 0 {
		t.Fatal("expected no stack before WithStackTrace")
	}
	e.WithStackTrace()
	if len(e.StackStace) == 0 {
		t.Fatal("expected non-empty StackStace after WithStackTrace")
	}
}

func TestStackTrace_NonEmpty(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewPanicStr("crash")
	e.WithStackTrace()

	st := e.StackTrace()
	if len(st) == 0 {
		t.Fatal("expected non-empty StackTrace")
	}
}

func TestStackTrace_Nil(t *testing.T) {
	var e *ErrMsg
	if e.StackTrace() != nil {
		t.Fatal("expected nil StackTrace for nil receiver")
	}
}

func TestStackTrace_NilStackStace(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = false
	e := NewNormalStr("m", "d")
	// StackStace is nil by default (stack disabled).
	if e.StackTrace() != nil {
		t.Fatal("expected nil StackTrace when no stack was captured")
	}
}

// ---------------------------------------------------------------------------
// Reset test
// ---------------------------------------------------------------------------

func TestErrMsg_Reset(t *testing.T) {
	baseenv.GetConfig().ErrorStackTrace = true
	e := NewDatabaseStr("info")
	e.Reset()

	if e.ErrMsg != "" || e.ErrInternalInfo != "" || e.StackStace != nil {
		t.Error("Reset() did not clear all fields")
	}
}
