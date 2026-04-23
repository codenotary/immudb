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

package sql

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"sort"
)

// distinctRowReader emits each distinct (by sha256 over rendered cols)
// row from rowReader exactly once.
//
// Two modes:
//
//   - Memory-only (default): the in-memory dedup map can grow up to
//     SQLTx.distinctLimit() entries, after which the next unique row
//     returns ErrTooManyRows. Same behaviour as before D5.
//
//   - Spill-enabled (engine option WithDistinctSpillThreshold > 0): when
//     the in-memory map reaches the threshold, its digests are sorted
//     and merged with any existing spill file into a fresh sorted spill
//     on disk; the in-memory map is then reset. Subsequent dedup checks
//     consult both the in-memory map and the spill via binary search,
//     keeping resident memory bounded regardless of total result size.
type distinctRowReader struct {
	rowReader RowReader
	cols      []ColDescriptor

	// in-memory dedup set; resets to empty after each spill flush.
	readRows map[[sha256.Size]byte]struct{}

	// spillThreshold is the engine's WithDistinctSpillThreshold (0 disables).
	spillThreshold int

	// Sorted spill file: sequence of sha256.Size-byte digests in ascending
	// byte order. spillCount is the number of entries currently in it.
	spillFile  *os.File
	spillCount int
}

func newDistinctRowReader(ctx context.Context, rowReader RowReader) (*distinctRowReader, error) {
	cols, err := rowReader.Columns(ctx)
	if err != nil {
		return nil, err
	}

	// rowReader.Tx() may be nil under test fixtures (dummyRowReader);
	// gracefully fall back to spill-disabled in that case so existing
	// unit tests don't see a nil-deref regression.
	threshold := 0
	if tx := rowReader.Tx(); tx != nil {
		threshold = tx.distinctSpillThreshold()
	}

	return &distinctRowReader{
		rowReader:      rowReader,
		cols:           cols,
		readRows:       make(map[[sha256.Size]byte]struct{}),
		spillThreshold: threshold,
	}, nil
}

func (dr *distinctRowReader) onClose(callback func()) {
	dr.rowReader.onClose(callback)
}

func (dr *distinctRowReader) Tx() *SQLTx {
	return dr.rowReader.Tx()
}

func (dr *distinctRowReader) TableAlias() string {
	return dr.rowReader.TableAlias()
}

func (dr *distinctRowReader) Parameters() map[string]interface{} {
	return dr.rowReader.Parameters()
}

func (dr *distinctRowReader) OrderBy() []ColDescriptor {
	return dr.rowReader.OrderBy()
}

func (dr *distinctRowReader) ScanSpecs() *ScanSpecs {
	return dr.rowReader.ScanSpecs()
}

func (dr *distinctRowReader) Columns(ctx context.Context) ([]ColDescriptor, error) {
	return dr.rowReader.Columns(ctx)
}

func (dr *distinctRowReader) colsBySelector(ctx context.Context) (map[string]ColDescriptor, error) {
	return dr.rowReader.colsBySelector(ctx)
}

func (dr *distinctRowReader) InferParameters(ctx context.Context, params map[string]SQLValueType) error {
	return dr.rowReader.InferParameters(ctx, params)
}

func (dr *distinctRowReader) Read(ctx context.Context) (*Row, error) {
	for {
		// Memory cap only applies when spill is disabled — with spill, the
		// in-memory map resets on flush so the cap loses meaning.
		if dr.spillThreshold == 0 && len(dr.readRows) == dr.rowReader.Tx().distinctLimit() {
			return nil, ErrTooManyRows
		}

		row, err := dr.rowReader.Read(ctx)
		if err != nil {
			return nil, err
		}

		digest, err := row.digest(dr.cols)
		if err != nil {
			return nil, err
		}

		if _, ok := dr.readRows[digest]; ok {
			continue
		}

		if dr.spillFile != nil {
			present, err := dr.spillContains(digest)
			if err != nil {
				return nil, err
			}
			if present {
				continue
			}
		}

		dr.readRows[digest] = struct{}{}

		if dr.spillThreshold > 0 && len(dr.readRows) >= dr.spillThreshold {
			if err := dr.flushToSpill(); err != nil {
				return nil, err
			}
		}

		return row, nil
	}
}

// flushToSpill sorts the in-memory digest set and merges it with the
// existing spill file (if any) into a fresh sorted spill file. Resets
// the in-memory map. Old spill file is deregistered + removed.
//
// Memory cost: O(threshold) for the sort buffer.
// I/O cost per flush: O(spillCount + threshold) sequential read + write.
// Amortized cost per emitted row over many flushes: O(N / threshold)
// where N is the eventual total spill size — append-only with periodic
// merge yields the best lookup latency at the price of write amplification.
func (dr *distinctRowReader) flushToSpill() error {
	sortedNew := make([][sha256.Size]byte, 0, len(dr.readRows))
	for d := range dr.readRows {
		sortedNew = append(sortedNew, d)
	}
	sort.Slice(sortedNew, func(i, j int) bool {
		return bytes.Compare(sortedNew[i][:], sortedNew[j][:]) < 0
	})

	tx := dr.rowReader.Tx()
	newFile, err := tx.createTempFile()
	if err != nil {
		return err
	}

	w := bufio.NewWriter(newFile)
	mergedCount, err := mergeSortedDigests(w, dr.spillFile, dr.spillCount, sortedNew)
	if err != nil {
		newFile.Close()
		return err
	}
	if err := w.Flush(); err != nil {
		newFile.Close()
		return err
	}

	if dr.spillFile != nil {
		// Deregister + close + remove the old spill so the surrounding
		// SQLTx doesn't keep a stale handle.
		oldFile := dr.spillFile
		tx.deregisterTempFile(oldFile)
		_ = oldFile.Close()
		_ = os.Remove(oldFile.Name())
	}

	dr.spillFile = newFile
	dr.spillCount = mergedCount
	dr.readRows = make(map[[sha256.Size]byte]struct{})
	return nil
}

// mergeSortedDigests writes the merged union of an existing on-disk
// sorted spill (existingFile, existingCount entries each sha256.Size
// bytes) and an in-memory sorted slice (newDigests) to w. Returns the
// total number of entries written. Duplicates between the two streams
// are emitted exactly once.
func mergeSortedDigests(w *bufio.Writer, existingFile *os.File, existingCount int, newDigests [][sha256.Size]byte) (int, error) {
	written := 0

	if existingFile == nil {
		for _, d := range newDigests {
			if _, err := w.Write(d[:]); err != nil {
				return written, err
			}
			written++
		}
		return written, nil
	}

	if _, err := existingFile.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	r := bufio.NewReader(existingFile)

	var existing [sha256.Size]byte
	haveExisting := false

	advanceExisting := func() error {
		if existingCount == 0 {
			haveExisting = false
			return nil
		}
		if _, err := io.ReadFull(r, existing[:]); err != nil {
			return err
		}
		existingCount--
		haveExisting = true
		return nil
	}

	if err := advanceExisting(); err != nil {
		return 0, err
	}

	i := 0
	for haveExisting && i < len(newDigests) {
		c := bytes.Compare(existing[:], newDigests[i][:])
		switch {
		case c < 0:
			if _, err := w.Write(existing[:]); err != nil {
				return written, err
			}
			written++
			if err := advanceExisting(); err != nil {
				return written, err
			}
		case c > 0:
			if _, err := w.Write(newDigests[i][:]); err != nil {
				return written, err
			}
			written++
			i++
		default: // equal
			if _, err := w.Write(existing[:]); err != nil {
				return written, err
			}
			written++
			if err := advanceExisting(); err != nil {
				return written, err
			}
			i++
		}
	}

	for haveExisting {
		if _, err := w.Write(existing[:]); err != nil {
			return written, err
		}
		written++
		if err := advanceExisting(); err != nil {
			return written, err
		}
	}
	for ; i < len(newDigests); i++ {
		if _, err := w.Write(newDigests[i][:]); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

// spillContains binary-searches the on-disk sorted spill file for digest.
// Each entry is sha256.Size bytes; offset = idx * sha256.Size.
func (dr *distinctRowReader) spillContains(digest [sha256.Size]byte) (bool, error) {
	lo, hi := 0, dr.spillCount
	var buf [sha256.Size]byte

	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if _, err := dr.spillFile.ReadAt(buf[:], int64(mid)*sha256.Size); err != nil {
			return false, err
		}
		c := bytes.Compare(buf[:], digest[:])
		switch {
		case c == 0:
			return true, nil
		case c < 0:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return false, nil
}

func (dr *distinctRowReader) Close() error {
	// The spill file (if present) is registered with the SQLTx via
	// createTempFile so the surrounding tx Cancel/Commit will clean it up
	// alongside our own deregister-on-flush pattern. No extra work needed
	// here — leaving the close to the tx avoids a double-close on the
	// remove-then-close ordering between flushToSpill and Close.
	return dr.rowReader.Close()
}
