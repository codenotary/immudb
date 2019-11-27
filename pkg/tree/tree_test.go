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
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testFrozen = []struct {
	at     uint64
	layer  uint8
	index  uint64
	frozen bool
}{
	{0, 0, 0, true},
	{6, 0, 7, false},
	{7, 0, 7, true},

	{6, 3, 0, false},
	{6, 2, 0, true}, {6, 2, 1, false},
	{6, 1, 0, true}, {6, 1, 1, true}, {6, 1, 2, true}, {6, 1, 3, false},
	{6, 0, 0, true}, {6, 0, 1, true}, {6, 0, 2, true}, {6, 0, 3, true}, {6, 0, 4, true}, {6, 0, 5, true}, {6, 0, 6, true}, {6, 0, 7, false},
}

func TestAppend(t *testing.T) {
	s := NewMemStore()
	assert.Equal(t, -1, Depth(s))

	for index := uint64(0); index <= 64; index++ {
		b := []byte(strconv.FormatUint(index, 10))
		Append(s, b)

		assert.Equal(t, index, uint64(s.Width()-1))
		d := int(math.Ceil(math.Log2(float64(index + 1))))
		assert.Equal(t, d, Depth(s))

		assert.Equal(t, testRoots[index], Root(s))
	}
}

func TestPrint(t *testing.T) {
	s := NewMemStore()
	for n := 0; n <= 64; n++ {
		Append(s, []byte(strconv.FormatUint(uint64(n), 10)))
		s.(*memStore).Print()
		fmt.Println("----------------------------------")
	}

}

func TestIsFrozen(t *testing.T) {
	for _, v := range testFrozen {
		assert.Equal(t, v.frozen, IsFrozen(v.layer, v.index, v.at))
	}
}

func TestPath(t *testing.T) {

	s := NewMemStore()
	D := [][]byte{}
	for index := uint64(0); index <= 64; index++ {
		v := []byte(strconv.FormatUint(index, 10))
		D = append(D, v)
		Append(s, v)

		// test out of range
		assert.Nil(t, PathAt(s, index+1, index))
		assert.Nil(t, PathAt(s, index, index+1))

		for at := uint64(0); at <= index; at++ {
			for i := uint64(0); i <= at; i++ {
				fmt.Printf("\n\n-----------------\nn=%d at=%d i=%d\n", index+1, at, i)
				path := PathAt(s, at, i)

				expected := MPath(i, D[0:at+1])

				if !assert.Len(t, path, len(expected)) {
					return
				}
				for k, v := range path {
					if !assert.Equal(t, expected[k], v) {
						return
					}
				}
			}
		}
	}
}

func TestVerify(t *testing.T) {

	path := Path{}
	assert.True(t, path.Verify(0, 0, [sha256.Size]byte{}, [sha256.Size]byte{}))

	assert.False(t, path.Verify(0, 1, [sha256.Size]byte{}, [sha256.Size]byte{}))
	assert.False(t, path.Verify(1, 0, [sha256.Size]byte{}, [sha256.Size]byte{}))
	assert.False(t, path.Verify(1, 1, [sha256.Size]byte{}, [sha256.Size]byte{}))

	s := NewMemStore()
	D := [][]byte{}
	for index := uint64(0); index <= 64; index++ {
		v := []byte(strconv.FormatUint(index, 10))
		D = append(D, v)
		Append(s, v)
		for at := uint64(0); at <= index; at++ {
			for i := uint64(0); i <= at; i++ {
				path := MPath(i, D[0:at+1])
				isV := Path(path).Verify(at, i, testRoots[at], *s.Get(0, i))
				assert.True(t, isV)
				if !isV {
					return
				}
			}
		}
	}
}

func BenchmarkLog2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		n := i
		_ = int(math.Ceil(math.Log2(float64(n))))
	}
}

func BenchmarkLog2bits(b *testing.B) {
	for i := 0; i < b.N; i++ {
		n := uint64(i)
		_ = bits.Len64(n - 1)
	}
}

func BenchmarkAppend(b *testing.B) {
	s := NewMemStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Append(s, []byte{0, 1, 3, 4, 5, 6, 7})
	}
}

func BenchmarkAppendHash(b *testing.B) {
	h := sha256.Sum256(nil)
	s := NewMemStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		AppendHash(s, &h)
	}
}

func BenchmarkAppendMap(b *testing.B) {
	s := NewMapStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Append(s, []byte{0, 1, 3, 4, 5, 6, 7})
	}
}

func BenchmarkPathAt(b *testing.B) {
	s := NewMemStore()
	for i := 0; i < b.N; i++ {
		Append(s, []byte{0, 1, 3, 4, 5, 6, 7})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PathAt(s, uint64(i), uint64(i))
	}
}
