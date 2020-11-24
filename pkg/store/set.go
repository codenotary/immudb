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

var _SetSeparator = []byte(`_~|IMMU|~_`)

// BuildSetKey composes the key of the set {separator}{set}{separator}{score}{key}{bit index presence flag}{index}
func BuildSetKey(key []byte, set []byte, score float64, index *schema.Index) (ik []byte) {
	i, s, vl, sl := len(set), binary.Size(score), len(key), len(_SetSeparator)
	c := make([]byte, i+s+vl+sl+sl)

	copy(c, _SetSeparator)
	copy(c[sl:], set)
	copy(c[sl+i:], _SetSeparator)
	copy(c[sl+i+sl:], Float642bytes(score))
	copy(c[sl+i+sl+s:], key[:])

	// append the timestamp to the set key. In this way equal keys with same score will be returned by timestamp
	return WrapZIndexReference(c, index)
}

// SetKeyScore return the score of an item given key and set name
func SetKeyScore(key []byte, set []byte) (score float64) {
	c := make([]byte, 8)
	copy(c, key[len(_SetSeparator)+len(set)+len(_SetSeparator):len(_SetSeparator)+len(set)+len(_SetSeparator)+8])
	return Bytes2float(c)
}

// AppendScore
func AppendScoreToSet(set []byte, score float64) []byte {
	i, s, sl := len(set), binary.Size(score), len(_SetSeparator)
	c := make([]byte, i+s+sl+sl)
	copy(c, _SetSeparator)
	copy(c[sl:], set)
	copy(c[sl+i:], _SetSeparator)
	copy(c[sl+i+sl:], Float642bytes(score))
	return c
}

// WrapSeparatorToSet
func WrapSeparatorToSet(set []byte) []byte {
	i, sl := len(set), len(_SetSeparator)
	c := make([]byte, i+sl+sl)
	copy(c, _SetSeparator)
	copy(c[sl:], set)
	copy(c[sl+i:], _SetSeparator)
	return c
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

// WrapZIndexReference if index is not nil this method append to the key the timestamp at the end of the reference and a bit flag 1.
// If index is not provided it append a 0 uint64 and a bit flag 0.
// Flag is put at the end of the key to ensure lexicographical capabilities
// This is needed to maintain compatibility with solutions that are not using the resolution facilities with timestamp
func WrapZIndexReference(key []byte, index *schema.Index) []byte {
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

// UnwrapZIndexReference returns the referenced key and the index of the key if provided in ZAdd or SafeZAdd operations
func UnwrapZIndexReference(reference []byte) (key []byte, flag byte, idx uint64) {
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
