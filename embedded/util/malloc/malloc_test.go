package malloc

import (
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestAlignBytes(t *testing.T) {
	tests := []struct {
		size      int
		alignment int
	}{
		{size: 4096, alignment: 4096},
		{size: 8192, alignment: 64},
		{size: 1024, alignment: 128},
		{size: 512, alignment: 32},
		{size: 256, alignment: 16},
	}

	for _, tt := range tests {
		b := make([]byte, tt.size+2*tt.alignment)
		off := rand.Intn(tt.alignment)

		bs := b[off : off+(tt.size+tt.alignment)]
		alignedBytes := alignBytes(bs, tt.size, tt.alignment)

		addr := uintptr(unsafe.Pointer(&alignedBytes[0]))

		require.True(t, addr%uintptr(tt.alignment) == 0)
		require.True(t, len(alignedBytes) == tt.size)
	}
}

func TestMallocAligned(t *testing.T) {
	tests := []struct {
		size      int
		alignment int
	}{
		{size: 4096, alignment: 4096},
		{size: 8192, alignment: 64},
		{size: 1024, alignment: 128},
		{size: 512, alignment: 32},
		{size: 256, alignment: 16},
	}

	for _, tt := range tests {
		slice := MallocAligned(tt.size, tt.alignment)
		addr := uintptr(unsafe.Pointer(&slice[0]))

		require.True(t, addr%uintptr(tt.alignment) == 0)
		require.True(t, len(slice) == tt.size)
	}
}

func TestMallocAlignedPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for non-power-of-two alignment, but function did not panic")
		}
	}()

	// This should panic because 100 is not a power of two.
	MallocAligned(1024, 100)
}
