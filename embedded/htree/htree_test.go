/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
package htree

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTree(t *testing.T) {
	const maxWidth = 1000

	_, err := New(0)
	require.Equal(t, ErrIllegalArguments, err)

	tree, err := New(maxWidth)
	require.NoError(t, err)
	require.NotNil(t, tree)

	_, err = tree.Root()
	require.Equal(t, ErrIllegalState, err)

	digests := make([][sha256.Size]byte, maxWidth)

	for i := 0; i < len(digests); i++ {
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(i))
		digests[i] = sha256.Sum256(b[:])
	}

	err = tree.BuildWith(digests)
	require.NoError(t, err)

	root, err := tree.Root()
	require.NoError(t, err)

	for i := 0; i < len(digests); i++ {
		proof, err := tree.InclusionProof(i)
		require.NoError(t, err)
		require.NotNil(t, proof)

		verifies := VerifyInclusion(proof, digests[i], root)
		require.True(t, verifies)

		verifies = VerifyInclusion(proof, sha256.Sum256(digests[i][:]), root)
		require.False(t, verifies)

		verifies = VerifyInclusion(proof, digests[i], sha256.Sum256(root[:]))
		require.False(t, verifies)

		proof.Terms = nil
		verifies = VerifyInclusion(proof, digests[i], root)
		require.False(t, verifies)

		verifies = VerifyInclusion(nil, digests[i], root)
		require.False(t, verifies)
	}

	err = tree.BuildWith(nil)
	require.Equal(t, ErrIllegalArguments, err)

	err = tree.BuildWith(make([][sha256.Size]byte, maxWidth+1))
	require.Equal(t, ErrMaxWidthExceeded, err)

	_, err = tree.InclusionProof(maxWidth)
	require.Equal(t, ErrIllegalArguments, err)
}
