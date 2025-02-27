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

	newTreeApp, err := t.appFactory(t.Path(), snapFolder("tree", t.Ts()), appOpts)
	if err != nil {
		return err
	}
	defer newTreeApp.Close()

	t.mtx.Lock()
	defer t.mtx.Unlock()

	_, err = t.flushTreeLog(
		ctx,
		t.rootPageID(),
		flushOptions{
			fullDump: true,
			dstApp:   newTreeApp,
		},
	)
	if err != nil {
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
