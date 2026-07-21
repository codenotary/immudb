//go:build !unix

package ledger

import (
	"fmt"
	"os"
	"time"
)

// acquireLock is a portable fallback for platforms without flock. It uses
// atomic O_CREATE|O_EXCL file creation as a mutex, breaking a lock that is
// older than the timeout (assumed to be from a crashed process).
func acquireLock(path string, timeout time.Duration) (release func(), err error) {
	deadline := time.Now().Add(timeout)
	for {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			f.Close()
			return func() { os.Remove(path) }, nil
		}
		if info, statErr := os.Stat(path); statErr == nil {
			if time.Since(info.ModTime()) > timeout {
				os.Remove(path) // stale lock from a dead process
				continue
			}
		}
		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("could not acquire ledger lock within %s", timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
