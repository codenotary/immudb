/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package tbtree

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
)

func (t *TBTree) Compact(ctx context.Context, force bool) error {
	if !t.compacting.CompareAndSwap(false, true) {
		return ErrCompactionInProgress
	}

	var compactionDone bool
	defer func() {
		// NOTE: After a successful compaction, the compaction flag remains true
		// to prevent additional compactions from running on the tree unless it is explicitly re-opened.
		t.compacting.Store(compactionDone)
	}()

	if !force && (t.StalePagePercentage() < t.compactionThld) {
		return ErrCompactionThresholdNotReached
	}

	appOpts := multiapp.DefaultOptions().
		WithReadOnly(t.readOnly).
		WithRetryableSync(false).
		WithFileSize(t.fileSize).
		WithFileMode(t.fileMode).
		WithWriteBufferSize(t.appWriteBufferSize).
		WithFileExt("t")

	snapRootID, snapTs, indexedEntries, err := func() (PageID, uint64, uint32, error) {
		t.mtx.Lock()
		defer t.mtx.Unlock()

		err := t.flushToTreeLog()
		// NOTE: Holding the lock on mtx while reading snapRootID and Ts ensures atomicity.
		return t.lastSnapshotRootID(), t.lastSnapshotTs.Load(), t.IndexedEntryCount(), err
	}()
	if err != nil {
		return err
	}

	if snapRootID == PageNone {
		return fmt.Errorf("attempting to compact an empty tree")
	}

	newTreeApp, err := t.appFactory(t.Path(), snapFolder("tree", snapTs), appOpts)
	if err != nil {
		return err
	}
	defer newTreeApp.Close()

	checksumApp := appendable.WithChecksum(newTreeApp)

	res, err := t.flushTreeLog(
		snapRootID,
		flushOptions{
			fullDump: true,
			dstApp:   checksumApp,
		},
	)
	if err != nil {
		return err
	}

	hLogSize, err := t.historyApp.Size()
	if err != nil {
		return err
	}

	ce := CommitEntry{
		Ts:                snapTs,
		TLogOff:           uint64(0),
		HLogOff:           uint64(hLogSize),
		HLogFlushedBytes:  0,
		TotalPages:        uint64(res.totalPagesFlushed),
		StalePages:        0,
		IndexedEntryCount: indexedEntries,
	}

	if err := commit(&ce, checksumApp); err != nil {
		return err
	}

	tLogOff, err := newTreeApp.Size()
	if err != nil {
		return err
	}

	// NOTE: we push an additional entry to avoid full rehashing of the treeApp during startup.
	newEntry := CommitEntry{
		Ts:                snapTs,
		TLogOff:           uint64(tLogOff),
		HLogOff:           uint64(hLogSize),
		HLogFlushedBytes:  0,
		TotalPages:        uint64(res.totalPagesFlushed),
		StalePages:        0,
		IndexedEntryCount: indexedEntries,
	}

	if err := commit(&newEntry, appendable.WithChecksum(t.treeApp)); err != nil {
		return err
	}

	if err := newTreeApp.Sync(); err != nil {
		return err
	}

	compactionDone = true
	return nil
}

func snapFolder(folder string, snapID uint64) string {
	if snapID == 0 {
		return folder
	}
	return fmt.Sprintf("%s_%016d", folder, snapID)
}

func parseSnapFolder(name string) (uint64, error) {
	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid snapshot folder name")
	}

	snapTs, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || parts[0] != TreeLogFileName {
		return 0, fmt.Errorf("invalid snapshot folder name")
	}
	return snapTs, nil
}
