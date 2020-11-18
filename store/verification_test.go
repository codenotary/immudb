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
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyLinearProofEdgeCases(t *testing.T) {
	require.False(t, VerifyLinearProof(nil, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))
	require.False(t, VerifyLinearProof(&LinearProof{}, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))

	require.True(t,
		VerifyLinearProof(
			&LinearProof{Proof: [][sha256.Size]byte{sha256.Sum256(nil)}, SourceTxID: 1, TargetTxID: 1},
			1,
			1,
			sha256.Sum256(nil),
			sha256.Sum256(nil),
		),
	)
}

func TestVerifyDualProofEdgeCases(t *testing.T) {
	require.False(t, VerifyDualProof(nil, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))
	require.False(t, VerifyDualProof(&DualProof{}, 0, 0, sha256.Sum256(nil), sha256.Sum256(nil)))
}
