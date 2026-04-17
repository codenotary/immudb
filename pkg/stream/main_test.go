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

package stream

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain (Q5) asserts that no test in this package leaks a goroutine
// past return. The pkg/stream surface is contained — no background
// tickers or HTTP servers — so unaccounted goroutines almost certainly
// indicate a bug in stream-handling code (e.g. an unbounded sender
// goroutine spawned by the A5 timeout path that the new test exercises).
//
// goleak's IgnoreTopFunction allowlist intentionally covers the goroutine
// that A5 sender_test.go's timeout path spawns: the slow-Send sleep is
// a deliberate test fixture and outlives the goroutine that observed
// ErrSendStalled. Listing it here is preferable to changing the test
// fixture, which would obscure the back-pressure scenario.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(
		m,
		// streamtest mock SendF blocks in a deliberate test sleep.
		goleak.IgnoreTopFunction("github.com/codenotary/immudb/pkg/stream_test.TestMsgSender_SendTimeoutFires.func1"),
		goleak.IgnoreTopFunction("time.Sleep"),
	)
}
