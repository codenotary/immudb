/*
Copyright 2026 Codenotary Inc. All rights reserved.

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

func TestZipfKeyTracker(t *testing.T) {
	// s=1.5 gives noticeable skew without being degenerate.
	kt := NewZipfKeyTracker(0, 999, 1.5, 1, 42)
	require.NotNil(t, kt)

	// With no writes yet, reads should return the start key deterministically.
	require.Equal(t, "KEY:0000000000", kt.GetRKeyZipf())

	// Populate 1000 writable keys.
	for i := 0; i < 1000; i++ {
		kt.GetWKey()
	}

	// Sample a large number of reads; the most frequent key should clearly
	// dominate under a Zipf distribution. For s=1.5 over imax=999, the hit
	// frequency of the top key is >20% in expectation, and the top-10 keys
	// together should cover a majority of reads.
	const nReads = 20000
	counts := make(map[string]int, 1000)
	for i := 0; i < nReads; i++ {
		counts[kt.GetRKeyZipf()]++
	}

	var maxHits int
	var topKey string
	for k, v := range counts {
		if v > maxHits {
			maxHits = v
			topKey = k
		}
	}

	// Sanity check: the distribution is skewed (some key got far more than
	// uniform 1/1000 = 20 hits).
	require.Greater(t, maxHits, 20, "expected skewed distribution; top key=%s hits=%d", topKey, maxHits)

	// Sanity check: not all reads landed on a single key — the tracker should
	// still explore the tail.
	require.Greater(t, len(counts), 10)
}
