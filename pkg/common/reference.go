/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"encoding/binary"
)

// WrapReferenceAt if atTx is not 0 this method append to the key the timestamp at the end of the reference and a bit flag 1.
// If atTx is not provided it append a 0 uint64 and a bit flag 0.
// Flag is put at the end of the key to ensure lexicographical capabilities
// This is needed to maintain compatibility with solutions that are not using the resolution facilities with timestamp
func WrapReferenceAt(key []byte, atTx uint64) []byte {
	var c = make([]byte, len(key)+8+1)
	copy(c, key)

	txB := make([]byte, 8)

	if atTx > 0 {
		binary.BigEndian.PutUint64(txB, atTx)
		copy(c[len(key):], txB)
		c[len(key)+8] = byte(1)

	} else {
		binary.BigEndian.PutUint64(txB, 0)
		copy(c[len(key):], txB)
		c[len(key)+8] = byte(0)
	}

	return c
}

// UnwrapReferenceAt returns the referenced key and an specific tx of the key if provided in Reference, SafeReference, ZAdd or SafeZAdd operations
func UnwrapReferenceAt(reference []byte) (key []byte, flag byte, atTx uint64) {
	key = make([]byte, len(reference)-8-1)
	copy(key, reference[:len(reference)-8-1])

	flag = reference[len(reference)-1]

	if flag == byte(1) {
		txB := make([]byte, 8)
		copy(txB, reference[len(reference)-8-1:])
		atTx = binary.BigEndian.Uint64(txB)
	}

	return
}
