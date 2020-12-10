package common

import (
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
)

// WrapIndexReference if index is not nil this method append to the key the timestamp at the end of the reference and a bit flag 1.
// If index is not provided it append a 0 uint64 and a bit flag 0.
// Flag is put at the end of the key to ensure lexicographical capabilities
// This is needed to maintain compatibility with solutions that are not using the resolution facilities with timestamp
func WrapIndexReference(key []byte, index *schema.Index) []byte {
	var c = make([]byte, len(key)+8+1)
	copy(c, key)
	if index != nil {
		idx := make([]byte, 8)
		binary.BigEndian.PutUint64(idx, index.Index)
		copy(c[len(key):], idx)
		c[len(key)+8] = byte(1)

	} else {
		idx := make([]byte, 8)
		binary.BigEndian.PutUint64(idx, 0)
		copy(c[len(key):], idx)
		c[len(key)+8] = byte(0)
	}
	return c
}

// UnwrapIndexReference returns the referenced key and the index of the key if provided in ZAdd or SafeZAdd operations
func UnwrapIndexReference(reference []byte) (key []byte, flag byte, idx uint64) {
	key = make([]byte, len(reference)-8-1)
	copy(key, reference[:len(reference)-8-1])
	flag = reference[len(reference)-1]
	if flag == byte(1) {
		idxB := make([]byte, 8)
		copy(idxB, reference[len(reference)-8-1:])
		idx = binary.BigEndian.Uint64(idxB)
	}
	return
}
