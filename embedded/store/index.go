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

package store

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/v2/embedded/tbtree"
	"github.com/codenotary/immudb/v2/embedded/watchers"
)

const indexDir = "index"

type EntryMapper func(key []byte, valueReader io.Reader) ([]byte, error)

type IndexSpec struct {
	SourcePrefix      []byte
	SourceEntryMapper EntryMapper

	TargetEntryMapper EntryMapper
	TargetPrefix      []byte

	InjectiveMapping        bool
	SourceIndexTargetPrefix []byte
}

func (spec *IndexSpec) Path(basePath string) string {
	if len(spec.TargetPrefix) == 0 {
		return filepath.Join(basePath, indexDir)
	}

	encPrefix := hex.EncodeToString(spec.TargetPrefix)
	return filepath.Join(basePath, fmt.Sprintf("%s_%s", indexDir, encPrefix))
}

type index struct {
	mtx sync.RWMutex

	path      string
	indexerID int
	closed    bool

	srcIdx *index
	ledger IndexableLedger

	spec IndexSpec
	tree *tbtree.TBTree
	wHub *watchers.WatchersHub
}

func (idx *index) mapKey(key []byte, valReader io.Reader) ([]byte, error) {
	if idx.spec.SourceEntryMapper == nil {
		return key, nil
	}
	return idx.spec.SourceEntryMapper(key, valReader)
}

func (idx *index) InsertAdvance(
	e tbtree.Entry,
	upToTs uint64,
	entryCount uint32,
) error {
	if !idx.mtx.TryRLock() {
		return tbtree.ErrTreeLocked
	}
	defer idx.mtx.RUnlock()

	return idx.tree.InsertAdvance(e, upToTs, entryCount)
}

func (idx *index) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}
	return idx.tree.Get(key)
}

func (idx *index) GetWithPrefix(key []byte, neq []byte) ([]byte, []byte, uint64, uint64, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}
	return idx.tree.GetWithPrefix(key, neq)
}

func (idx *index) GetBetween(key []byte, initialTxID uint64, finalTxID uint64) (value []byte, tx uint64, hc uint64, err error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	if err := idx.tree.Flush(); err != nil {
		return nil, 0, 0, err
	}
	return idx.tree.GetBetween(key, initialTxID, finalTxID)
}

func (idx *index) History(key []byte, offset uint64, descOrder bool, limit int) (timedValues []tbtree.TimedValue, hCount uint64, err error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, 0, ErrAlreadyClosed
	}

	if err := idx.tree.Flush(); err != nil {
		return nil, 0, err
	}
	return idx.tree.History(key, offset, descOrder, limit)
}

func (idx *index) Snapshot() (tbtree.Snapshot, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}
	return idx.SnapshotMustIncludeTx(context.Background(), 0)
}

func (idx *index) WriteSnapshot() (tbtree.Snapshot, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}
	return idx.tree.WriteSnapshot()
}

func (idx *index) ID() tbtree.TreeID {
	return idx.tree.ID()
}

func (idx *index) EntriesIndexedAtTs(ts uint64) uint32 {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	currTs := idx.tree.Ts()

	switch {
	case ts <= currTs:
		return math.MaxUint32
	case ts == currTs+1:
		return idx.tree.IndexedEntryCount()
	case ts > currTs+1:
		return 0
	}
	return 0
}

// Must be called with close guard already acquired
func (idx *index) advanceTs(ts uint64, lastEntry uint32) error {
	if err := idx.tree.AdvanceTs(ts, lastEntry); err != nil {
		return err
	}
	return idx.wHub.DoneUpto(ts)
}

func (idx *index) WaitForIndexingUpTo(ctx context.Context, txID uint64) error {
	if idx.wHub != nil {
		err := idx.wHub.WaitFor(ctx, txID)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return ErrAlreadyClosed
		}
		return err
	}
	return watchers.ErrMaxWaitessLimitExceeded
}

func (idx *index) SnapshotMustIncludeTx(ctx context.Context, txID uint64) (tbtree.Snapshot, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}
	return idx.SnapshotMustIncludeTxWithRenewalPeriod(ctx, txID, 0)
}

func (idx *index) SnapshotMustIncludeTxWithRenewalPeriod(ctx context.Context, txID uint64, renewalPeriod time.Duration) (tbtree.Snapshot, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}

	// TODO: consider renewal period
	return idx.tree.SnapshotMustIncludeTsWithRenewalPeriod(ctx, txID, renewalPeriod)
}

func (idx *index) SnapshotAtTs(ctx context.Context, txID uint64) (tbtree.Snapshot, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return nil, ErrAlreadyClosed
	}
	// TODO: consider renewal period
	return idx.tree.SnapshotAtTs(ctx, txID)
}

func (idx *index) Flush() error {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if idx.closed {
		return ErrAlreadyClosed
	}
	return idx.tree.Flush()
}

func (idx *index) Compact(ctx context.Context, force bool) error {
	err := func() error {
		idx.mtx.RLock()
		defer idx.mtx.RUnlock()

		if idx.closed {
			return ErrAlreadyClosed
		}
		return idx.tree.Compact(ctx, force)
	}()
	if err != nil {
		return err
	}

	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	err = idx.tree.Close()
	if err != nil {
		return err
	}

	idx.tree, err = tbtree.Open(idx.path, idx.tree.GetOptions())
	return err
}

func (idx *index) IndexingLag() uint64 {
	return idx.ledger.LastCommittedTxID() - idx.tree.Ts()
}

func (idx *index) shouldIndex() bool {
	return idx.IndexingLag() > 0
}

func (idx *index) Ts() uint64 {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	return idx.tree.Ts()
}

func (idx *index) SourcePrefix() []byte {
	return idx.spec.SourcePrefix
}

func (idx *index) TargetPrefix() []byte {
	return idx.spec.TargetPrefix
}

func (idx *index) Close() error {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	if idx.closed {
		return ErrAlreadyClosed
	}

	idx.wHub.Close()

	err := idx.tree.Close()
	if err == nil {
		idx.closed = true
	}
	return err
}

func (idx *index) Closed() bool {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	return idx.closed
}
