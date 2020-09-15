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
	"math"
)

// SetKey ...
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
