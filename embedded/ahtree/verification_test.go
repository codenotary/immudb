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

	"github.com/stretchr/testify/require"
)

func TestVerificationEdgeCases(t *testing.T) {
	require.False(t, VerifyInclusion(nil, 1, 10, sha256.Sum256(nil), sha256.Sum256(nil)))
	require.False(t, VerifyInclusion(nil, 10, 1, sha256.Sum256(nil), sha256.Sum256(nil)))

	require.False(t, VerifyConsistency(nil, 1, 10, sha256.Sum256(nil), sha256.Sum256(nil)))
	require.False(t, VerifyConsistency(nil, 10, 1, sha256.Sum256(nil), sha256.Sum256(nil)))
}
