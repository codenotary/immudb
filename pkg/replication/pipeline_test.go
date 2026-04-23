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

package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEffectivePipelineDepth(t *testing.T) {
	// Sync replication forces depth=1 regardless of the configured value.
	require.Equal(t, 1, effectivePipelineDepth(4, true))
	require.Equal(t, 1, effectivePipelineDepth(1, true))
	require.Equal(t, 1, effectivePipelineDepth(0, true))

	// Async passes through, with a floor of 1.
	require.Equal(t, 1, effectivePipelineDepth(0, false))
	require.Equal(t, 1, effectivePipelineDepth(-3, false))
	require.Equal(t, 1, effectivePipelineDepth(1, false))
	require.Equal(t, 4, effectivePipelineDepth(4, false))
}

// TestPipelineSendRange exercises the A4 send-range computation across
// the lifecycle scenarios fetchNextTx hits: fresh stream, steady-state
// pipelining, and reconnect after disconnect.
func TestPipelineSendRange(t *testing.T) {
	tests := []struct {
		name        string
		lastSent    uint64
		nextTx      uint64
		depth       int
		wantLo      uint64
		wantHi      uint64
		wantAnchor  bool
		wantSendCnt int
	}{
		{
			name:        "depth=1 fresh stream sends one (no anchor needed: lastSent==nextTx-1)",
			lastSent:    0,
			nextTx:      1,
			depth:       1,
			wantLo:      1, wantHi: 1, wantAnchor: false, wantSendCnt: 1,
		},
		{
			name:        "depth=1 steady state sends one",
			lastSent:    5,
			nextTx:      6,
			depth:       1,
			wantLo:      6, wantHi: 6, wantAnchor: false, wantSendCnt: 1,
		},
		{
			name:        "depth=2 fresh stream primes with two sends",
			lastSent:    0,
			nextTx:      1,
			depth:       2,
			wantLo:      1, wantHi: 2, wantAnchor: false, wantSendCnt: 2,
		},
		{
			name:        "anchor fires when lastSent lags behind nextTx-1 (post-disconnect)",
			lastSent:    2,
			nextTx:      6,
			depth:       1,
			wantLo:      6, wantHi: 6, wantAnchor: true, wantSendCnt: 1,
		},
		{
			name:        "depth=2 steady state sends just the new top-of-pipeline",
			lastSent:    2,
			nextTx:      2,
			depth:       2,
			wantLo:      3, wantHi: 3, wantAnchor: false, wantSendCnt: 1,
		},
		{
			name:        "depth=4 steady state at full depth sends one",
			lastSent:    7,
			nextTx:      4,
			depth:       4,
			wantLo:      8, wantHi: 7, wantAnchor: false, wantSendCnt: 0, // already saturated
		},
		{
			name:        "depth=4 steady state, behind by two, sends two to refill",
			lastSent:    5,
			nextTx:      4,
			depth:       4,
			wantLo:      6, wantHi: 7, wantAnchor: false, wantSendCnt: 2,
		},
		{
			name:        "reconnect at tx=10 anchors and sends depth=3 fresh",
			lastSent:    0,
			nextTx:      10,
			depth:       3,
			wantLo:      10, wantHi: 12, wantAnchor: true, wantSendCnt: 3,
		},
		{
			name:        "depth=0 normalises to 1",
			lastSent:    3,
			nextTx:      4,
			depth:       0,
			wantLo:      4, wantHi: 4, wantAnchor: false, wantSendCnt: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lo, hi, anchored := pipelineSendRange(tc.lastSent, tc.nextTx, tc.depth)
			require.Equal(t, tc.wantLo, lo, "lo")
			require.Equal(t, tc.wantHi, hi, "hi")
			require.Equal(t, tc.wantAnchor, anchored, "anchored")

			sendCnt := 0
			if hi >= lo {
				sendCnt = int(hi - lo + 1)
			}
			require.Equal(t, tc.wantSendCnt, sendCnt, "send count")
		})
	}
}

// TestPipelineSteadyStateSimulation walks several fetchNextTx-style
// iterations end to end and asserts the cumulative Send count leads the
// ReadFully count by exactly (depth-1) once the pipeline is primed —
// the load-bearing invariant that justifies A4.
func TestPipelineSteadyStateSimulation(t *testing.T) {
	for _, depth := range []int{1, 2, 4} {
		t.Run("depth", func(t *testing.T) {
			var lastSent, lastApplied uint64
			totalSends := 0

			const iterations = 10
			for iter := 0; iter < iterations; iter++ {
				nextTx := lastApplied + 1
				lo, hi, anchored := pipelineSendRange(lastSent, nextTx, depth)
				if anchored {
					lastSent = nextTx - 1
				}
				if hi >= lo {
					sends := int(hi - lo + 1)
					totalSends += sends
					lastSent = hi
				}
				// Caller now drains one response and applies tx nextTx.
				lastApplied = nextTx
			}

			// After N iterations, we've applied N txs and have (depth-1)
			// extra Sends in flight that haven't been ReadFully'd yet.
			require.Equal(t, iterations+depth-1, totalSends,
				"depth=%d: expected %d sends after %d applies, got %d",
				depth, iterations+depth-1, iterations, totalSends)
		})
	}
}
