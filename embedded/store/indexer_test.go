package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/appendable"
	memapp "github.com/codenotary/immudb/embedded/appendable/memory"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"

	"github.com/stretchr/testify/require"
)

func TestInitIndex(t *testing.T) {
	nIndexes := 1
	readTxAt := func(txID uint64, tx *Tx) error {
		entries := make([]*TxEntry, nIndexes)
		for i := range entries {
			key := []byte(fmt.Sprintf("prefix%d:key%d", i, txID))
			value := []byte(fmt.Sprintf("value-%d", txID))

			entries[i] = &TxEntry{
				k:    key,
				kLen: len(key),
				vLen: len(value),
				hVal: sha256.Sum256(value),
				vOff: int64(txID),
			}
		}

		tx.header = &TxHeader{
			ID:       txID,
			Metadata: &TxMetadata{},
			NEntries: nIndexes,
		}
		tx.entries = entries

		return nil
	}

	writeBufferSize := 128 * 1024 * 1024
	pageBufferSize := 1024 * 1024

	indexOptions := DefaultIndexOptions().
		WithNumIndexers(1).
		WithSharedWriteBufferSize(writeBufferSize).
		WithMaxWriteBufferSize(writeBufferSize).
		WithPageBufferSize(pageBufferSize)

	opts := DefaultOptions().
		WithIndexOptions(indexOptions).
		WithAppFactoryFunc(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	ledger := NewMockLedger("", opts, readTxAt)

	idx, err := NewIndexerManager(
		opts,
	)
	require.NoError(t, err)

	idx.Start()

	_, err = idx.GetIndexFor(ledger.ID(), nil)
	require.ErrorIs(t, err, ErrIndexNotFound)

	index, err := idx.InitIndexing(ledger, IndexSpec{})
	require.NoError(t, err)
	require.NotNil(t, index)

	_, err = idx.InitIndexing(ledger, IndexSpec{})
	require.ErrorIs(t, err, ErrIndexAlreadyInitialized)
}

func TestCloseIndexing(t *testing.T) {
	writeBufferSize := 1024 * 1024
	pageBufferSize := tbtree.PageSize * 5

	indexOptions := DefaultIndexOptions().
		WithNumIndexers(1).
		WithSharedWriteBufferSize(writeBufferSize).
		WithMaxWriteBufferSize(writeBufferSize).
		WithPageBufferSize(pageBufferSize)

	opts := DefaultOptions().
		WithIndexOptions(indexOptions).
		WithAppFactoryFunc(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	idxManager, err := NewIndexerManager(opts)
	require.NoError(t, err)

	defer idxManager.Close()

	idxManager.Start()

	ledger := NewMockLedger("", opts, readTxAtFor(1))

	idx, err := idxManager.InitIndexing(ledger, IndexSpec{})
	require.NoError(t, err)

	ledger.DoneUpTo(10)
	idxManager.NotifyTransactions(10)

	err = idxManager.WaitForIndexingUpTo(context.Background(), ledger.ID(), 10)
	require.NoError(t, err)

	t.Run("closing index with active snapshots should fail", func(t *testing.T) {
		snap, err := idx.tree.SnapshotMustIncludeTs(context.Background(), 10)
		require.NoError(t, err)
		defer snap.Close()

		_, err = idxManager.CloseIndexing(ledger.ID(), nil)
		require.ErrorIs(t, err, tbtree.ErrActiveSnapshots)
	})

	t.Run("closing index should succeeed", func(t *testing.T) {
		_, err = idxManager.CloseIndexing(ledger.ID(), nil)
		require.NoError(t, err)
	})

	t.Run("closing an already closed index should fail", func(t *testing.T) {
		_, err = idxManager.CloseIndexing(ledger.ID(), nil)
		require.ErrorIs(t, err, ErrAlreadyClosed)
	})

	_, err = idxManager.GetIndexFor(ledger.ID(), nil)
	require.ErrorIs(t, err, ErrIndexNotFound)

	err = idxManager.WaitForIndexingUpTo(context.Background(), ledger.ID(), 100)
	require.NoError(t, err)

	// retrigger indexing
	ledger.DoneUpTo(11)
	idxManager.NotifyTransactions(11)

	t.Run("index should eventually be removed from indexer queue", func(t *testing.T) {
		require.Eventually(t, func() bool {
			indexer := &idxManager.indexers[0]
			return indexer.Indexes() == 0
		}, time.Second, time.Millisecond*10)
	})
}

func TestIndexers(t *testing.T) {
	nIndexes := 100

	writeBufferSize := 8 * 1024 * 1024
	pageBufferSize := tbtree.PageSize * 5

	indexOptions := DefaultIndexOptions().
		WithNumIndexers(8).
		WithSharedWriteBufferSize(writeBufferSize).
		WithMaxWriteBufferSize(writeBufferSize).
		WithPageBufferSize(pageBufferSize)

	opts := DefaultOptions().
		WithIndexOptions(indexOptions).
		WithAppFactoryFunc(func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			return memapp.New(), nil
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	ledger := NewMockLedger("", opts, readTxAtFor(nIndexes))

	idx, err := NewIndexerManager(opts)
	require.NoError(t, err)

	idx.Start()

	ensureIndexedUpTo := func(idx *index, txID uint64) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		err := idx.WaitForIndexingUpTo(ctx, txID)
		require.NoError(t, err)
		require.Equal(t, txID, idx.Ts())

		snap, err := idx.SnapshotMustIncludeTx(ctx, txID)
		require.NoError(t, err)

		// TODO: check snapshot ts
		for i := uint64(1); i <= txID; i++ {
			mkey, err := idx.mapKey([]byte(fmt.Sprintf("mprefix%d:key%d", idx.tree.ID(), i)), nil)
			require.NoError(t, err)

			err = snap.UseEntry(mkey, func(e *tbtree.Entry) error {
				hval := e.Value[12 : 12+sha256.Size]
				expectedHVal := sha256.Sum256([]byte(fmt.Sprintf("value-%d", i)))
				require.Equal(t, expectedHVal[:], hval)
				return nil
			})
			require.NoError(t, err)
		}
	}

	indexes := make([]*index, nIndexes)

	for n := 0; n < nIndexes; n++ {
		prefix := fmt.Sprintf("prefix%d:", n)

		index, err := idx.InitIndexing(ledger, IndexSpec{
			SourcePrefix: []byte(prefix),
			TargetPrefix: []byte(strings.Replace(prefix, "prefix", "mprefix", 1)),
			SourceEntryMapper: func(key []byte, _ io.Reader) ([]byte, error) {
				return key, nil
			},
			TargetEntryMapper: func(key []byte, _ io.Reader) ([]byte, error) {
				return []byte(strings.Replace(string(key), "prefix", "mprefix", 1)), nil
			},
		})
		require.NoError(t, err)

		indexes[n] = index
	}

	nTransactions := uint64(1 << 12)
	ledger.DoneUpTo(nTransactions)
	idx.NotifyTransactions(nTransactions)

	var wg sync.WaitGroup
	wg.Add(nIndexes)
	for _, idx := range indexes {
		go func(idx *index) {
			defer wg.Done()

			ensureIndexedUpTo(idx, nTransactions)
		}(idx)
	}
	wg.Wait()
}

func TestIndexingRecovery(t *testing.T) {
	treeApp := memapp.New()

	opts := DefaultOptions().
		WithAppFactoryFunc(func(_, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			switch subPath {
			case "tree":
				return treeApp, nil
			case "history":
				return memapp.New(), nil
			}
			return nil, fmt.Errorf("invalid subpath: %s", subPath)
		}).
		WithReadDirFunc(func(path string) ([]os.DirEntry, error) {
			return nil, nil
		})

	ledger := NewMockLedger("", opts, readTxAtFor(1))

	idx, err := NewIndexerManager(opts)
	require.NoError(t, err)

	idx.Start()

	index, err := idx.InitIndexing(ledger, IndexSpec{})
	require.NoError(t, err)

	upToTx := uint64(500)
	ledger.DoneUpTo(upToTx)
	idx.NotifyTransactions(upToTx)

	ctx := context.Background()

	err = index.WaitForIndexingUpTo(ctx, upToTx)
	require.NoError(t, err)

	err = index.Flush()
	require.NoError(t, err)

	upToTx = uint64(1000)
	ledger.DoneUpTo(upToTx)
	idx.NotifyTransactions(upToTx)

	err = index.WaitForIndexingUpTo(ctx, upToTx)
	require.NoError(t, err)

	err = idx.Close()
	require.NoError(t, err)

	t.Run("recovery after proper shutdown", func(t *testing.T) {
		idx, err = NewIndexerManager(opts)
		require.NoError(t, err)

		idx.Start()

		index, err = idx.InitIndexing(ledger, IndexSpec{})
		require.NoError(t, err)

		require.Equal(t, upToTx, index.Ts())

		err = idx.Close()
		require.NoError(t, err)
	})

	t.Run("recovery after crash", func(t *testing.T) {
		size, err := treeApp.Size()
		require.NoError(t, err)

		newSize := size - 1

		err = treeApp.SetOffset(newSize)
		require.NoError(t, err)

		idx, err = NewIndexerManager(opts)
		require.NoError(t, err)

		idx.Start()

		index, err = idx.InitIndexing(ledger, IndexSpec{})
		require.NoError(t, err)

		require.Equal(t, upToTx/2, index.Ts())
	})
}

func readTxAtFor(nIndexes int) func(txID uint64, tx *Tx) error {
	return func(txID uint64, tx *Tx) error {
		entries := make([]*TxEntry, nIndexes)
		for i := range entries {
			key := []byte(fmt.Sprintf("prefix%d:key%d", i, txID))
			value := []byte(fmt.Sprintf("value-%d", txID))

			entries[i] = &TxEntry{
				k:    key,
				kLen: len(key),
				vLen: len(value),
				hVal: sha256.Sum256(value),
				vOff: int64(txID),
			}
		}

		tx.header = &TxHeader{
			ID:       txID,
			Metadata: &TxMetadata{},
			NEntries: nIndexes,
		}
		tx.entries = entries

		return nil
	}
}

type MockLedger struct {
	path              string
	opts              *Options
	commitWh          *watchers.WatchersHub
	lastCommittedTxID uint64
	readTxAt          func(txID uint64, tx *Tx) error
}

func NewMockLedger(path string, opts *Options, readTxAt func(txID uint64, tx *Tx) error) *MockLedger {
	return &MockLedger{
		path:              path,
		opts:              opts,
		commitWh:          watchers.New(0, maxWaitingDefault),
		lastCommittedTxID: 0,
		readTxAt:          readTxAt,
	}
}

func (l *MockLedger) ID() LedgerID {
	return 0
}

func (l *MockLedger) Path() string {
	return l.path
}

func (l *MockLedger) DoneUpTo(txID uint64) {
	err := l.commitWh.DoneUpto(txID)
	if err != nil {
		panic(err)
	}
}

func (l *MockLedger) LastCommittedTxID() uint64 {
	doneUpTo, _, err := l.commitWh.Status()
	if err != nil {
		panic(err)
	}
	return doneUpTo
}

func (l *MockLedger) WaitFor(ctx context.Context, txID uint64) error {
	return l.commitWh.WaitFor(ctx, txID)
}

func (l *MockLedger) ReadTxAt(txID uint64, tx *Tx) error {
	if l.readTxAt == nil {
		return fmt.Errorf("ReadTxAt: no read function specified")
	}
	return l.readTxAt(txID, tx)
}

func (l *MockLedger) ReadTxEntry(txID uint64, key []byte, skipIntegrityCheck bool) (*TxEntry, *TxHeader, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (l *MockLedger) ValueReaderAt(vlen int, off int64, hvalue [sha256.Size]byte, skipIntegrityCheck bool) (io.Reader, error) {
	v := fmt.Sprintf("value-%d", off)
	if vlen != len(v) {
		return nil, fmt.Errorf("value size doens't match buffer size")
	}
	return bytes.NewReader([]byte(v)), nil
}

func (l *MockLedger) Options() *Options {
	return l.opts
}
