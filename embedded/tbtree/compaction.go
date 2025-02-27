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

		err := t.flushToTreeLog(ctx)
		// NOTE: Holding the lock on mtx while reading rootPageID and Ts ensures atomicity.
		return t.rootPageID(), t.Ts(), err
	}()
	if err != nil {
		return err
	}

	newTreeApp, err := t.appFactory(t.Path(), snapFolder("tree", snapTs), appOpts)
	if err != nil {
		return err
	}
	defer newTreeApp.Close()

	res, err := t.flushTreeLog(
		ctx,
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
		HLogSize:          uint64(hLogSize),
		TotalPages:        uint64(res.pagesFlushed),
		StalePages:        0,
		IndexedEntryCount: t.IndexedEntryCount(),
	}

	if err := commit(&ce, newTreeApp); err != nil {
		return err
	}

	t.pgBuf.InvalidateTreePages(t.ID())
	return nil
}

func snapFolder(folder string, snapID uint64) string {
	if snapID == 0 {
		return folder
	}
	return fmt.Sprintf("%s_%016d", folder, snapID)
}
