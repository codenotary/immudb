/*
Copyright 2019 vChain, Inc.

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

package tree

import (
	"crypto/sha256"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMTH(t *testing.T) {

	D := [][]byte{}
	assert.Equal(t, sha256.Sum256(nil), MTH(D))
	for n := 0; n <= 64; n++ {
		b := []byte(strconv.FormatUint(uint64(n), 10))
		D = append(D, b)
		assert.Equal(t, testRoots[n], MTH(D))
	}
}

func TestMPath(t *testing.T) {

	D := [][]byte{}
	for n := uint64(0); n <= 8; n++ {
		b := []byte(strconv.FormatUint(uint64(n), 10))
		D = append(D, b)
		for i := uint64(0); i <= n; i++ {
			path := MPath(i, D)
			// fmt.Printf("TEST(n=%d): i=%d\n", n, i)
			// for d, h := range path {
			// 	fmt.Printf("%d) %.2x\n", d, h[0])
			// }
			// fmt.Println("---------------------------")
			// fmt.Println()
			assert.Equal(t, testPaths[n][i], path)
		}
	}
}

func BenchmarkMTH(b *testing.B) {
	D := [][]byte{}
	for i := 0; i < b.N; i++ {
		D = append(D, []byte{0, 1, 3, 4, 5, 6, 7})
		MTH(D)
	}
}
