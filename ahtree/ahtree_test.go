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

package ahtree

import (
	"crypto/sha256"
	"testing"

	"github.com/codenotary/merkletree"
	"github.com/stretchr/testify/require"
)

func TestAHtree(t *testing.T) {
	tree := &AHtree{}

	var nodesUptoTests = []struct {
		n        uint64
		expected uint64
	}{
		{1, 1},
		{2, 3},
		{3, 5},
		{4, 8},
		{5, 10},
		{6, 13},
		{7, 16},
		{8, 20},
		{9, 22},
		{10, 25},
		{11, 28},
		{12, 32},
		{13, 35},
		{14, 39},
		{15, 43},
		{16, 48},
	}

	for _, tt := range nodesUptoTests {
		actual := nodesUpto(tt.n)
		require.Equal(t, tt.expected, actual)

		require.Equal(t, tt.expected, nodesUntil(tt.n)+uint64(1+levelsAt(tt.n)))
	}

	N := 16

	for i := 1; i <= N; i++ {
		_, _, err := tree.Append([]byte{byte(i)})
		require.NoError(t, err)

		ri, _ := tree.RootAt(uint64(i))
		r, err := tree.Root()
		require.NoError(t, err)
		require.Equal(t, r, ri)
	}

	for i := 1; i <= N; i++ {
		for j := i; i <= N; j++ {
			proof, err := tree.InclusionProof(uint64(i), uint64(j))
			require.NoError(t, err)

			root, _ := tree.RootAt(uint64(j))

			h := sha256.Sum256([]byte{byte(i)})

			verifies := merkletree.Path(proof).VerifyInclusion(uint64(j), uint64(i), root, h)
			require.True(t, verifies)
		}
	}

}
