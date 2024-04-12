package slices

import (
	"unsafe"
)

// BytesToString converts bytes to a string without memory allocation.
// NOTE: The given bytes MUST NOT be modified since they share the same backing array
// with the returned string.
func BytesToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
