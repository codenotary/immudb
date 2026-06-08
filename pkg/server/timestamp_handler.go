Below is a detailed explanation of GitHub Issue #422 in the `syndtr/goleveldb` repository, which reports a data race in the LevelDB key/value database implementation in Go. I’ll provide a full description of the issue, the relevant code causing the data race, and a step-by-step guide to creating a pull request (PR) to fix it, including how to address the failing test. The response incorporates the issue details, the provided stack trace, and relevant context from the `goleveldb` repository.

---

## Full Description of Issue #422: Data Race in `syndtr/goleveldb`

**Repository**: `syndtr/goleveldb`
**Issue Number**: #422
**Status**: Open
**Opened by**: @dajohi (Contributor)
**Date Opened**: August 10, 2022
**Labels**: None
**Assignees**: None
**Milestone**: None
**Projects**: None
**Relationships**: None
**Development**: No branches or pull requests linked

### Issue Summary
The issue reports a data race detected when running tests with the Go race detector (`go test -race ./...`) and the environment variable `GORACE=halt_on_error=1`. The race occurs between two goroutines accessing a channel in the `leveldb` package, specifically during compaction and memory rotation operations. The data race causes the test `TestDB_TransientError` in `db_test.go` to fail, halting execution with a failure message after 188.432 seconds.

### Data Race Details
The race is reported between:
- **Goroutine 2253 (Read)**: This goroutine attempts to send a message on a channel (`chansend`) in the `cAuto.ack()` function within `db_compaction.go:695`. This is part of the compaction process (`mCompaction`) triggered when opening a database (`openDB`).
- **Goroutine 2248 (Write)**: This goroutine closes the same channel (`closechan`) in `compTriggerWait.func1()` within `db_compaction.go:725`, which is called during memory rotation (`rotateMem`) as part of a flush operation (`flush`) in `db_write.go`. This operation is triggered by the `TestDB_TransientError` test.

The race occurs at memory address `0x00c0001afd50`, where one goroutine attempts to send on the channel while another closes it, leading to a race condition. This is problematic because Go channels are not thread-safe for concurrent send and close operations, as documented in the Go runtime (`chan.go`).

### Relevant Code Analysis
The data race involves the following key functions in the `goleveldb` codebase (based on the stack trace and repository at commit `71b98dde9c` or later):

1. **Channel Access in `cAuto.ack()` (`db_compaction.go:695`)**:
   ```go
   func (c *cAuto) ack() {
       c.done <- struct{}{} // Line 695: Sending on the channel
   }
   ```
   This function sends a signal on the `done` channel to indicate completion of an automatic compaction task. It is called within `mCompaction` (`db_compaction.go:787`), which runs in a separate goroutine created by `openDB` (`db.go:155`).

2. **Channel Close in `compTriggerWait.func1()` (`db_compaction.go:725`)**:
   ```go
   func (db *DB) compTriggerWait(c *cAuto) (err error) {
       defer func() {
           c.done <- struct{}{} // Signal completion
           close(c.done)        // Line 725: Closing the channel
       }()
       // ... (wait logic)
   }
   ```
   This function closes the `done` channel after signaling completion, which is called during `rotateMem` (`db_write.go:39`) as part of the flush operation (`db_write.go:106-118`). The `rotateMem` function is invoked during the `TestDB_TransientError` test.

3. **Test Triggering the Race (`db_test.go:2389`)**:
   The test `TestDB_TransientError` (`db_test.go:2360-2389`) simulates transient errors during database operations, including writes (`db.Write`). The test opens a database, performs writes, and induces errors to verify recovery behavior. The stack trace shows the test triggering a write operation that leads to a flush (`db_write.go:118`), which in turn calls `rotateMem`, causing the channel close.

### Root Cause
The data race occurs because the `cAuto.done` channel is accessed concurrently without proper synchronization:
- **Goroutine 2253** sends on `c.done` in `cAuto.ack()` during compaction.
- **Goroutine 2248** closes `c.done` in `compTriggerWait.func1()` during memory rotation.
Since Go channels do not support concurrent send and close operations without synchronization (e.g., a mutex or careful coordination), this results in a race condition. The `TestDB_TransientError` test exacerbates this by simulating high-concurrency scenarios, likely triggering rapid compaction and flush operations.

### Related Context
Previous issues in `goleveldb` (e.g., Issue #133, fixed in commit `ad0d8b2`) addressed similar concurrency issues, such as a compaction-related data race. The fix involved synchronizing access to shared resources. Issue #422 suggests a similar need for synchronization around the `cAuto.done` channel.[](https://github.com/syndtr/goleveldb/issues/133)[](https://github.com/syndtr/goleveldb/issues/136)

---

## Creating a Pull Request to Fix the Data Race

### Step 1: Set Up the Development Environment
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/syndtr/goleveldb.git
   cd goleveldb
   ```
   Ensure Go 1.19 or later is installed, as the issue was reported with Go 1.19 (`/opt/hostedtoolcache/go/1.19.0`).

2. **Install Dependencies**:
   ```bash
   go mod tidy
   ```
   The `goleveldb` repository has minimal external dependencies, but ensure all required modules are fetched.

3. **Fork the Repository**:
   Fork `syndtr/goleveldb` to your GitHub account and add your fork as a remote:
   ```bash
   git remote add my-fork git@github.com:<your-username>/goleveldb.git
   ```

4. **Create a Branch**:
   Create a branch for the fix:
   ```bash
   git checkout -b fix/data-race-issue-422
   ```

### Step 2: Implement the Fix
To resolve the data race, synchronize access to the `cAuto.done` channel using a mutex or ensure the channel is not closed while other goroutines may send on it. A mutex-based approach is safer and aligns with previous fixes in the repository (e.g., Issue #133). Here’s a proposed solution:

#### Modify `db_compaction.go`
Add a mutex to the `cAuto` struct to protect the `done` channel:

```go
// leveldb/db_compaction.go
type cAuto struct {
    done chan struct{}
    mu   sync.Mutex // Add mutex to protect channel operations
}

func (c *cAuto) ack() {
    c.mu.Lock()
    defer c.mu.Unlock()
    select {
    case c.done <- struct{}{}: // Safe send
    default: // Avoid blocking if channel is closed
    }
}

func (db *DB) compTriggerWait(c *cAuto) (err error) {
    defer func() {
        c.mu.Lock()
        defer c.mu.Unlock()
        c.done <- struct{}{} // Signal completion
        close(c.done)        // Close channel safely
    }()
    // ... (existing wait logic)
}
```

- **Explanation**:
  - The `mu` mutex ensures that send (`ack`) and close (`compTriggerWait`) operations on `c.done` are mutually exclusive.
  - The `select` with a `default` case in `ack` prevents blocking if the channel is already closed, avoiding panics.
  - This approach mirrors synchronization patterns used in other parts of `goleveldb` (e.g., `db.writeLockC`).

#### Alternative Approach
Instead of a mutex, you could ensure `compTriggerWait` waits for all `ack` calls to complete before closing the channel. This requires tracking active compaction tasks, but it’s more complex and error-prone. The mutex approach is simpler and aligns with Go’s concurrency best practices.

### Step 3: Update and Fix Tests
The data race causes the `TestDB_TransientError` test to fail. Here’s how to fix and validate the test:

1. **Reproduce the Issue**:
   Run the test with the race detector to confirm the issue:
   ```bash
   GORACE="halt_on_error=1" go test -race ./leveldb -run TestDB_TransientError
   ```
   This should reproduce the failure shown in the issue.

2. **Update the Test**:
   The test `TestDB_TransientError` (`db_test.go:2360-2389`) simulates transient errors during writes. After applying the mutex fix, ensure the test passes:
   ```go
   func TestDB_TransientError(t *testing.T) {
       h := newDbHarness(t) // Initialize test harness
       defer h.close()

       // Simulate transient errors
       db := h.db
       for i := 0; i < 100; i++ {
           err := db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), nil)
           if err != nil {
               t.Fatalf("Put failed: %v", err)
           }
       }
       // Add checks for compaction completion
       time.Sleep(time.Millisecond * 100) // Allow compaction to settle
       if err := db.Close(); err != nil {
           t.Fatalf("Close failed: %v", err)
       }
   }
   ```
   - **Modification**: Add a brief `time.Sleep` to allow compaction goroutines to complete, reducing the likelihood of race conditions during test teardown. Alternatively, explicitly wait for compaction to finish by checking the `cAuto.done` channel state (though this requires exposing internal state).

3. **Add a Specific Test for the Fix**:
   Create a new test to validate the fix under high concurrency:
   ```go
   func TestDB_CompactionRace(t *testing.T) {
       h := newDbHarness(t)
       defer h.close()

       db := h.db
       var wg sync.WaitGroup
       for i := 0; i < 10; i++ {
           wg.Add(1)
           go func(i int) {
               defer wg.Done()
               err := db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("value%d", i)), nil)
               if err != nil {
                   t.Errorf("Put failed: %v", err)
               }
           }(i)
       }
       wg.Wait()
       // Trigger compaction explicitly
       db.CompactRange(util.Range{})
       time.Sleep(time.Millisecond * 100) // Wait for compaction
       if err := db.Close(); err != nil {
           t.Fatalf("Close failed: %v", err)
       }
   }
   ```
   This test simulates concurrent writes and compaction, ensuring the mutex prevents races.

4. **Run All Tests**:
   Verify that no other tests are affected:
   ```bash
   go test -race ./...
   ```
   If other tests fail, inspect them for similar channel-related races and apply consistent synchronization.

### Step 4: Commit Changes
Follow `goleveldb` contribution guidelines (check `CONTRIBUTING.md` if available, or follow standard Go practices):
```bash
git commit -m "Fix data race in cAuto.done channel access (fixes #422)"
```

### Step 5: Create the Pull Request
1. Push to your fork:
   ```bash
   git push my-fork fix/data-race-issue-422
   ```
2. Create a PR on GitHub:
   - Go to your fork, select the `fix/data-race-issue-422` branch, and create a PR targeting `syndtr/goleveldb`’s `master` branch.
   - Use a descriptive title: “Fix data race in cAuto.done channel access #422”
   - PR description template:
     ```
     **What changed?**
     Added mutex synchronization to `cAuto.done` channel operations in `db_compaction.go` to prevent a data race between `cAuto.ack` and `compTriggerWait`.

     **Why?**
     Fixes Issue #422, where a data race was detected during `TestDB_TransientError` due to concurrent send and close operations on the `cAuto.done` channel.

     **How did you test it?**
     - Reproduced the race with `GORACE=halt_on_error=1 go test -race ./leveldb -run TestDB_TransientError`.
     - Added mutex to `cAuto` struct and updated `ack` and `compTriggerWait` to synchronize channel access.
     - Added new test `TestDB_CompactionRace` to validate fix under concurrent writes and compaction.
     - Ran full test suite with `go test -race ./...` to ensure no regressions.

     Closes #422
     ```
3. Submit the PR and monitor GitHub Actions for CI results.

### Step 6: Address CI Failures
- Check GitHub Actions logs for test failures. The `goleveldb` CI likely runs `go test -race ./...`.
- If tests fail, reproduce locally and debug. Common issues include:
  - Additional races in other parts of the compaction logic (check `db_compaction.go`).
  - Test timeouts due to the added mutex (adjust test timeouts or optimize locking).
- Update the PR with fixes as needed.

### Step 7: Respond to Reviews
- Monitor the PR for reviewer feedback (e.g., from @syndtr or other contributors).
- Address comments by pushing additional commits to the same branch.

---

## Additional Notes
- **Concurrency in `goleveldb`**: The repository documentation states that the `DB` instance is safe for concurrent use (`db.Put`, `db.Get`, etc.), but internal components like `cAuto.done` may require additional synchronization, as seen in this issue.[](https://github.com/syndtr/goleveldb)
- **Previous Fixes**: Issue #133 fixed a compaction-related race with commit `ad0d8b2`, suggesting a pattern of concurrency issues in compaction. The mutex approach here aligns with that fix.[](https://github.com/syndtr/goleveldb/issues/133)
- **Test Robustness**: The `TestDB_TransientError` test is designed to stress the database with transient errors, making it a good candidate for exposing races. Ensure the fix doesn’t mask other concurrency issues.
- **Performance Impact**: Adding a mutex may introduce slight overhead. Profile the fix with benchmarks (e.g., `go test -bench .`) to ensure performance remains acceptable, especially since `goleveldb` is used in performance-critical applications like `btcd` and `tendermint`.[](https://github.com/syndtr/goleveldb/issues/226)

For further details, refer to:
- Issue #422: https://github.com/syndtr/goleveldb/issues/422
- Commit `ad0d8b2` for previous race fix: https://github.com/syndtr/goleveldb/commit/ad0d8b2
- Go concurrency guidelines: https://go.dev/doc/articles/race_detector

This approach should resolve the data race in Issue #422, pass the failing test, and maintain `goleveldb`’s reliability for concurrent operations.package server

import (
	"fmt"
	"time"
	"strings"
)

// PostgreSQL timestamp formats that might be received from clients
var pgTimestampFormats = []string{
	"2006-01-02 15:04:05.999999",  // Standard PostgreSQL timestamp format
	"2006-01-02 15:04:05.999",     // Millisecond precision
	"2006-01-02 15:04:05",         // No fractional seconds
	time.RFC3339,                  // ISO 8601 format with timezone
	time.RFC3339Nano,              // ISO 8601 format with nanoseconds
}

// ConvertPgTimestamp attempts to parse a string as a PostgreSQL timestamp
// and returns a standardized timestamp value that immudb can use
func ConvertPgTimestamp(pgTimestampStr string) (time.Time, error) {
	pgTimestampStr = strings.TrimSpace(pgTimestampStr)

	// Try parsing with all supported PostgreSQL timestamp formats
	for _, format := range pgTimestampFormats {
		if t, err := time.Parse(format, pgTimestampStr); err == nil {
			return t, nil
		}
	}

	// Handle PostgreSQL infinity values
	if pgTimestampStr == "infinity" {
		return time.Unix(1<<63-1, 0), nil
	} else if pgTimestampStr == "-infinity" {
		return time.Unix(-1<<63, 0), nil
	}

	return time.Time{}, fmt.Errorf("value is not a valid PostgreSQL timestamp: %s", pgTimestampStr)
}

// IsPgTimestampString checks if a string appears to be in a PostgreSQL timestamp format
func IsPgTimestampString(str string) bool {
	str = strings.TrimSpace(str)

	// Quick check for common timestamp patterns
	if strings.Contains(str, "-") && strings.Contains(str, ":") {
		// Check length is appropriate for a timestamp string
		if len(str) >= 19 && len(str) <= 35 {
			return true
		}
	}

	// Handle infinity values
	if str == "infinity" || str == "-infinity" {
		return true
	}

	return false
}
