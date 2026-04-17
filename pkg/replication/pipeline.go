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

// effectivePipelineDepth normalises the configured A4 pipeline depth.
// Returns 1 for sync replication (which carries per-request ReplicaState
// the primary uses for commit acks; pre-sending stale state corrupts
// that handshake) and clamps to a minimum of 1 otherwise.
func effectivePipelineDepth(configured int, syncReplicationEnabled bool) int {
	if syncReplicationEnabled {
		return 1
	}
	if configured < 1 {
		return 1
	}
	return configured
}

// pipelineSendRange returns the [lo, hi] range of tx IDs that
// fetchNextTx should Send on exportTxStream during a single call, given
// the prior in-flight high-watermark (lastSentTx), the next tx the
// caller will ReadFully (nextTx), and the configured pipeline depth.
//
// anchored=true signals that lastSentTx was BELOW nextTx-1 (the stream
// reconnected, so previous in-flight sends are gone) and the caller
// should reset lastSentTx = nextTx - 1 to match the returned range.
//
// Examples:
//
//	depth=1, fresh: pipelineSendRange(0, 1, 1) → (1, 1, true)  // 1 Send
//	depth=2, fresh: pipelineSendRange(0, 1, 2) → (1, 2, true)  // 2 Sends prime the pipeline
//	depth=2, mid:   pipelineSendRange(2, 2, 2) → (3, 3, false) // 1 Send keeps it full
//	depth=4, mid:   pipelineSendRange(5, 3, 4) → (6, 6, false) // 1 Send to reach lastTx+depth=7? actually lastSentTx=5 covers 3..5, need 6
//
// Returns lo > hi (an empty range) when the pipeline already has
// enough in flight to satisfy the depth target — the caller's for-loop
// then issues no Sends.
func pipelineSendRange(lastSentTx, nextTx uint64, depth int) (lo, hi uint64, anchored bool) {
	if depth < 1 {
		depth = 1
	}
	if lastSentTx < nextTx-1 {
		// Reconnect / fresh stream: previously-sent requests are lost
		// with the old stream. Anchor at nextTx-1 so the loop emits
		// requests starting from nextTx itself.
		lastSentTx = nextTx - 1
		anchored = true
	}
	hi = nextTx + uint64(depth-1)
	lo = lastSentTx + 1
	return lo, hi, anchored
}
