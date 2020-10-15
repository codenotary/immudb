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

package store

import (
	"encoding/binary"
	"github.com/codenotary/immudb/pkg/api/schema"
	"math"
)

// SetKey composes the key of the set {set}{score}{key}
func SetKey(key []byte, set []byte, score float64) (ik []byte, err error) {
	i, s, vl := len(set), binary.Size(score), len(key)
	c := make([]byte, i+s+vl)
	copy(c, set)
	copy(c[i:], Float642bytes(score))
	copy(c[i+s:], key[:]) // array to slice conversion. shorthand for x[0:len(x)]
	return c, nil
}

// Float642bytes ...
func Float642bytes(float float64) []byte {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, bits)
	return bytes
}

// Bytes2float ...
func Bytes2float(bytes []byte) float64 {
	bits := binary.BigEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

// WrapZIndexReference if index is not nil this method append to the key a bit 1 and and the timestamp at the end of the reference.
// If index is not provided it append 0 and a 0 uint64
// this is needed to maintain compatibility with solution that are not using the resolution facilities with timestamp
func WrapZIndexReference(key []byte, index *schema.Index) []byte {
	var c = make([]byte, len(key)+1+8)
	copy(c, key)
	if index != nil {
		c[len(key)] = byte(1)
		idx := make([]byte, 8)
		binary.BigEndian.PutUint64(idx, index.Index)
		copy(c[len(key)+1:], idx)
	} else {
		c[len(key)] = byte(0)
		idx := make([]byte, 8)
		binary.BigEndian.PutUint64(idx, 0)
		copy(c[len(key)+1:], idx)
	}
	return c
}

// UnwrapZIndexReference returns the referenced key and the index of the key if provided in ZAdd or SafeZAdd operations
func UnwrapZIndexReference(reference []byte) (key []byte, flag byte, idx uint64) {
	var c = make([]byte, len(reference)-1-8)
	copy(c, reference[:len(reference)-1-8])
	key = c
	flag = reference[len(reference)-1-8]
	if flag == byte(1) {
		idxb := make([]byte, 8)
		copy(idxb, reference[len(reference)-8:])
		idx = binary.BigEndian.Uint64(idxb)
	}
	return
}
