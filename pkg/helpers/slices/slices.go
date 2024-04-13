package slices

import (
	"unsafe"
)

// BytesToString converts bytes to a string without memory allocation.
// NOTE: The given bytes MUST NOT be modified since they share the same backing array
// with the returned string.
// Reference implementation: https://github.com/golang/go/blob/ad7c32dc3b6d5edc3dd72b3e15c80dc4f4c27064/src/strings/builder.go#L47.
func BytesToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
