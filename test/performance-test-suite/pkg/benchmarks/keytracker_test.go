/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package benchmarks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyTracker(t *testing.T) {
	kt := NewKeyTracker(0)
	require.NotNil(t, kt)

	k := kt.GetRKey()
	require.Equal(t, "KEY:0000000000", k)

	k = kt.GetWKey()
	require.Equal(t, "KEY:0000000000", k)

	k = kt.GetWKey()
	require.Equal(t, "KEY:0000000001", k)

	k = kt.GetWKey()
	require.Equal(t, "KEY:0000000002", k)

	usedKeys := map[string]int{}
	for i := 0; i < 100; i++ {
		k := kt.GetRKey()
		require.Contains(t,
			[]string{
				"KEY:0000000000",
				"KEY:0000000001",
				"KEY:0000000002",
			},
			k,
		)
		usedKeys[k]++
	}

	require.Len(t, usedKeys, 3)
	for _, u := range usedKeys {
		require.Greater(t, u, 20)
	}
}
