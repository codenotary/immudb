package store

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/codenotary/immudb/embedded/multierr"
)

var (
	ErrMaxNumberOfLedgersExceeded = errors.New("max number of ledgers exceeded")
	ErrLedgerNotExists            = errors.New("ledger not exists")
	ErrLedgerExists               = errors.New("ledger already exists")
)

type ImmuStore struct {
	opts *Options
	path string

	nextLedgerID atomic.Uint32

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

	if !finfo.IsDir() {
		return nil, ErrPathIsNotADirectory
	}

	// TODO: list path and open ledgers
	// setup next ledger id
	st := &ImmuStore{
		path:           path,
		opts:           opts,
		indexerManager: indexerManager,
	}

	//ledgers, err := openAllLedgers(path, st)
	//if err != nil {
	//	return nil, err
	//}

	st.ledgers = make(map[string]*Ledger)
	indexerManager.Start()

	return st, nil
}

/*
func openAllLedgers(path string, st *ImmuStore) (map[string]*Ledger, error) {
	ledgers := make(map[string]*Ledger)

	finfo, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ledgers, nil
	}
	if err != nil {
		return nil, err
	}

	if !finfo.IsDir() {
		return nil, ErrPathIsNotADirectory
	}

	dirEntries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return ledgers, nil
	}
	if err != nil {
		return nil, err
	}

	for _, entry := range dirEntries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		ledger, err := openLedger(name, st, nil)
		if err != nil {
			return nil, err
		}

		ledgers[name] = ledger
	}
	return ledgers, nil
}*/

func (st *ImmuStore) getNextLedgerID() (LedgerID, error) {
	newID := st.nextLedgerID.Add(1)
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

func (st *ImmuStore) removeLedger(name string) error {
	st.mtx.Lock()
	defer st.mtx.Unlock()

	delete(st.ledgers, name)
	return nil
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
