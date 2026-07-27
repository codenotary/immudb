//go:build !unix

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

package ledger

import (
	"fmt"
	"os"
	"time"
)

// staleBreakAfter is how old a lock file must be before it is assumed to belong
// to a crashed process and forcibly broken. It is deliberately far larger than
// the acquire timeout (and than any real ledger operation) so a legitimately
// long-running operation can never have its live lock stolen — the failure mode
// Codex flagged for the naive "break anything older than the timeout" approach.
const staleBreakAfter = 10 * time.Minute

// acquireLock is a portable fallback for platforms without flock. It uses
// atomic O_CREATE|O_EXCL file creation as a mutex. A lock is only broken once it
// is older than staleBreakAfter (assumed to be from a crashed process), not
// merely older than the acquire timeout.
func acquireLock(path string, timeout time.Duration) (release func(), err error) {
	deadline := time.Now().Add(timeout)
	for {
		f, oerr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if oerr == nil {
			f.Close()
			return func() { os.Remove(path) }, nil
		}

		if info, statErr := os.Stat(path); statErr == nil && time.Since(info.ModTime()) > staleBreakAfter {
			// Lock from a long-dead process. Retry immediately only if the
			// removal actually succeeded: if it did not (read-only directory,
			// a denying ACL, another process holding an open handle) the next
			// iteration would reproduce this exact state, so falling through
			// to the deadline check and the sleep below is what keeps this
			// from becoming a tight infinite loop that ignores the timeout.
			if rerr := os.Remove(path); rerr == nil {
				continue
			}
		}

		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("could not acquire ledger lock within %s: %w", timeout, oerr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
