package malloc

import (
	"unsafe"
)

// MallocAligned allocates a `size`-byte slice aligned to `alignment` bytes.
//
// It over-allocates memory, then adjusts the start address to the next aligned boundary.
// The function ensures `alignment` is a power of two and returns a properly aligned slice.
func MallocAligned(size, alignment int) []byte {
	if alignment <= 0 || alignment&(alignment-1) != 0 {
		panic("alignment must be a power of two")
	}

	raw := make([]byte, size+alignment)
	return alignBytes(raw, size, alignment)
}

func alignBytes(raw []byte, size, alignment int) []byte {
	rawPtr := uintptr(unsafe.Pointer(&raw[0]))
	alignedPtr := (rawPtr + uintptr(alignment-1)) &^ uintptr(alignment-1)
	offset := int(alignedPtr - rawPtr)

	return raw[offset : offset+size]
}
