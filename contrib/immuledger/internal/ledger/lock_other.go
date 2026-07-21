//go:build !unix

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
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			f.Close()
			return func() { os.Remove(path) }, nil
		}
		if info, statErr := os.Stat(path); statErr == nil {
			if time.Since(info.ModTime()) > staleBreakAfter {
				os.Remove(path) // lock from a long-dead process
				continue
			}
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("could not acquire ledger lock within %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
