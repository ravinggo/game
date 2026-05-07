// Package utils_test contains tests for the utils package.
// Written by Claude Code claude-opus-4-6.
package utils

import (
	"os"
	"testing"
)

// ---------------------------------------------------------------------------
// IsDirExists tests
// ---------------------------------------------------------------------------

func TestIsDirExists_ExistingDir(t *testing.T) {
	// os.TempDir() is guaranteed to exist on all platforms.
	if !IsDirExists(os.TempDir()) {
		t.Errorf("expected IsDirExists to return true for %q", os.TempDir())
	}
}

func TestIsDirExists_MissingDir(t *testing.T) {
	missing := os.TempDir() + "/this_path_should_never_exist_utils_test_12345"
	if IsDirExists(missing) {
		t.Errorf("expected IsDirExists to return false for non-existent path %q", missing)
	}
}

func TestIsDirExists_CreatedThenRemoved(t *testing.T) {
	dir, err := os.MkdirTemp("", "utils_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	if !IsDirExists(dir) {
		t.Errorf("expected IsDirExists to return true for freshly created dir %q", dir)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatalf("failed to remove temp dir: %v", err)
	}
	if IsDirExists(dir) {
		t.Errorf("expected IsDirExists to return false after dir removal")
	}
}

// ---------------------------------------------------------------------------
// BytesToString / StringToBytes roundtrip tests
// ---------------------------------------------------------------------------

func TestBytesToString_Basic(t *testing.T) {
	input := []byte("hello world")
	s := BytesToString(input)
	if s != "hello world" {
		t.Errorf("expected 'hello world', got %q", s)
	}
}

func TestBytesToString_Empty(t *testing.T) {
	s := BytesToString([]byte{})
	if s != "" {
		t.Errorf("expected empty string, got %q", s)
	}
}

func TestStringToBytes_Basic(t *testing.T) {
	input := "hello world"
	b := StringToBytes(input)
	if string(b) != input {
		t.Errorf("expected %q, got %q", input, string(b))
	}
}

func TestStringToBytes_Empty(t *testing.T) {
	b := StringToBytes("")
	if len(b) != 0 {
		t.Errorf("expected zero-length slice, got len=%d", len(b))
	}
}

func TestBytesStringRoundtrip(t *testing.T) {
	cases := []string{
		"",
		"a",
		"hello, world!",
		"unicode: 中文",
		string(make([]byte, 1024)),
	}
	for _, tc := range cases {
		b := StringToBytes(tc)
		s := BytesToString(b)
		if s != tc {
			t.Errorf("roundtrip mismatch: input=%q, output=%q", tc, s)
		}
	}
}

func TestStringBytesLengthConsistency(t *testing.T) {
	s := "test string 123"
	b := StringToBytes(s)
	if len(b) != len(s) {
		t.Errorf("expected len(bytes)=%d to match len(string)=%d", len(b), len(s))
	}
	if BytesToString(b) != s {
		t.Error("conversion back to string failed")
	}
}

// ---------------------------------------------------------------------------
// Allocation-free behaviour (informal, not enforced by assertion)
// ---------------------------------------------------------------------------

func BenchmarkBytesToString(b *testing.B) {
	data := []byte("benchmark string for zero allocation check")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BytesToString(data)
	}
}

func BenchmarkStringToBytes(b *testing.B) {
	s := "benchmark string for zero allocation check"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StringToBytes(s)
	}
}
