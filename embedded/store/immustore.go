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
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/codenotary/immudb/v2/embedded/multierr"
)

var (
	ErrMaxNumberOfLedgersExceeded = errors.New("max number of ledgers exceeded")
	ErrLedgerNotExists            = errors.New("ledger not exists")
	ErrLedgerExists               = errors.New("ledger already exists")
)

type ImmuStore struct {
	opts *Options
	path string

	nextLedgerID uint32

	mtx     sync.RWMutex
	closed  bool
	ledgers map[string]*Ledger

	indexerManager *IndexerManager
}

func Open(path string, opts *Options) (*ImmuStore, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	indexerManager, err := NewIndexerManager(opts)
	if err != nil {
		return nil, err
	}

	finfo, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return nil, err
	}

	if finfo != nil && !finfo.IsDir() {
		return nil, ErrPathIsNotADirectory
	}

	st := &ImmuStore{
		path:           path,
		opts:           opts,
		indexerManager: indexerManager,
		ledgers:        make(map[string]*Ledger),
	}

	indexerManager.Start()

	return st, nil
}

func (st *ImmuStore) getNextLedgerID() (LedgerID, error) {
	newID := atomic.AddUint32(&st.nextLedgerID, 1)
	if newID-1 > newID {
		return 0, ErrMaxNumberOfLedgersExceeded
	}
	return LedgerID(newID) - 1, nil
}

func (st *ImmuStore) OpenLedger(name string) (*Ledger, error) {
	return st.OpenLedgerWithOpts(name, st.opts)
}

func (st *ImmuStore) OpenLedgerWithOpts(name string, opts *Options) (*Ledger, error) {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	ledger := st.ledgers[name]
	if ledger != nil && !ledger.IsClosed() {
		return ledger, nil
	}

	ledger, err := openLedger(name, st, opts)
	if err != nil {
		return nil, err
	}

	st.ledgers[name] = ledger
	return ledger, nil
}

func (st *ImmuStore) GetLedgerByName(name string) (*Ledger, error) {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	ledger, exists := st.ledgers[name]
	if !exists {
		return ledger, fmt.Errorf("%s: %w", name, ErrLedgerNotExists)
	}
	return ledger, nil
}

func (st *ImmuStore) IsClosed() bool {
	st.mtx.RLock()
	defer st.mtx.RUnlock()

	return st.closed
}

func (st *ImmuStore) Close() error {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	if st.closed {
		return ErrAlreadyClosed
	}

	merr := multierr.NewMultiErr()
	for _, ledger := range st.ledgers {
		if err := ledger.Close(); !errors.Is(err, ErrAlreadyClosed) {
			merr.Append(err)
		}
	}
	err := st.indexerManager.Close()
	return merr.Append(err).Reduce()
}
