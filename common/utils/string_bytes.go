package utils

import (
	"unsafe"
)

// BytesToString convert bytes to string
// Please pay attention to the life cycle of the object when using this function
func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// StringToBytes convert string to bytes
// return-value read-only
// Please pay attention to the life cycle of the object when using this function
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
