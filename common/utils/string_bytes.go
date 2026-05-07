package utils

import (
	"unsafe"
)

// BytesToString convert bytes to string
// Please pay attention to the life cycle of the object when using this function.
// The returned string shares memory with the original slice; the slice must not
// be modified or garbage-collected while the string is in use.
// Written by Claude Code claude-opus-4-6.
func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// StringToBytes convert string to bytes
// return-value read-only
// Please pay attention to the life cycle of the object when using this function.
// The returned slice shares memory with the original string and must not be
// written to. The string must remain live for the duration of any use of the
// slice.
// Written by Claude Code claude-opus-4-6.
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
