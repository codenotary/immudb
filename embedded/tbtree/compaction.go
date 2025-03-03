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

	"github.com/codenotary/immudb/embedded/appendable/multiapp"
)

func (t *TBTree) Compact(ctx context.Context) error {
	if !t.compacting.CompareAndSwap(false, true) {
		return ErrCompactionInProgress
	}

	var compactionDone bool
	defer func() {
		// NOTE: After a successful compaction, the compaction flag remains true
		// to prevent additional compactions from running on the tree unless it is explicitly re-opened.
		t.compacting.Store(compactionDone)
	}()

	if t.StalePagePercentage() < t.compactionThld {
		return ErrCompactionThresholdNotReached
	}

	appOpts := multiapp.DefaultOptions().
		WithReadOnly(t.readOnly).
		WithRetryableSync(false).
		WithFileSize(t.fileSize).
		WithFileMode(t.fileMode).
		WithWriteBufferSize(t.appWriteBufferSize).
		WithFileExt("t")

	snapRootID, snapTs, err := func() (PageID, uint64, error) {
		t.mtx.Lock()
		defer t.mtx.Unlock()

		err := t.flushToTreeLog()
		// NOTE: Holding the lock on mtx while reading snapRootID and Ts ensures atomicity.
		return t.lastSnapshotRootID(), t.lastSnapshotTs.Load(), err
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

	res, err := t.flushTreeLog(
		snapRootID,
		flushOptions{
			fullDump: true,
			dstApp:   newTreeApp,
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
		HLogOff:           uint64(hLogSize),
		TotalPages:        uint64(res.totalPages),
		StalePages:        0,
		IndexedEntryCount: t.IndexedEntryCount(),
	}

	if err := commit(&ce, nil); err != nil {
		return err
	}
	compactionDone = true

	t.pgBuf.InvalidateTreePages(t.ID())
	return nil
}

func snapFolder(folder string, snapID uint64) string {
	if snapID == 0 {
		return folder
	}
	return fmt.Sprintf("%s_%016d", folder, snapID)
}
