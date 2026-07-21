//go:build unix

package ledger

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

// acquireLock takes an exclusive advisory lock on a lock file so that only one
// immuledger process touches the embedded immudb data directory at a time
// (immudb itself does not lock the directory). flock is released automatically
// if the process dies, so no stale-lock handling is needed.
func acquireLock(path string, timeout time.Duration) (release func(), err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	deadline := time.Now().Add(timeout)
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if !time.Now().Before(deadline) {
			f.Close()
			return nil, fmt.Errorf("could not acquire ledger lock within %s: %w", timeout, err)
		}
		time.Sleep(50 * time.Millisecond)
	}

	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
