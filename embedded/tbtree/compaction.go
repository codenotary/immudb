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

type CompactionJob struct {
	tree   *TBTree
	closed bool
}

func (t *TBTree) NewCompactionJob() (*CompactionJob, error) {
	if !t.mtx.TryLock() {
		return nil, ErrTreeLocked
	}

	if !t.snapshotLock.TryLock() {
		t.mtx.Unlock()
		return nil, ErrActiveSnapshots
	}

	return &CompactionJob{
		tree: t,
	}, nil
}

func (job *CompactionJob) Run(ctx context.Context) error {
	err := job.runCompaction(ctx)
	job.close()
	return err
}

func (job *CompactionJob) Abort() {
	job.close()
}

func (job *CompactionJob) Path() string {
	return job.tree.Path()
}

func (job *CompactionJob) close() {
	if job.closed {
		return
	}

	job.tree.snapshotLock.Unlock()
	job.tree.mtx.Unlock()
	job.closed = true
}

func (job *CompactionJob) runCompaction(ctx context.Context) error {
	tree := job.tree
	appOpts := multiapp.DefaultOptions().
		WithReadOnly(job.tree.readOnly).
		WithRetryableSync(false).
		WithFileSize(tree.fileSize).
		WithFileMode(tree.fileMode).
		WithWriteBufferSize(tree.appWriteBufferSize).
		WithFileExt("t")

	newTreeApp, err := tree.appFactory(tree.path, snapFolder("tree", tree.Ts()), appOpts)
	if err != nil {
		return err
	}

	err = tree.flush(
		ctx,
		flushOptions{fullDump: true, dstApp: newTreeApp},
	)
	if err != nil {
		return err
	}

	oldTreeApp := tree.treeApp
	tree.treeApp = newTreeApp

	_ = oldTreeApp.Close()

	tree.pgBuf.InvalidateTreePages(tree.ID())
	return nil
}

func snapFolder(folder string, snapID uint64) string {
	if snapID == 0 {
		return folder
	}
	return fmt.Sprintf("%s_%016d", folder, snapID)
}
