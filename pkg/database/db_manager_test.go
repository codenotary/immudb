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

package database

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
)

type mockDB struct {
	DB

	name string
}

func (db *mockDB) GetName() string {
	return db.name
}

func (db *mockDB) Close() error {
	return nil
}

func (db *mockDB) GetOptions() *Options {
	return &Options{}
}

func (db *mockDB) CurrentState() (*schema.ImmutableState, error) {
	return &schema.ImmutableState{}, nil
}

func openMockDB(name string, opts *Options) (DB, error) {
	return &mockDB{name: name}, nil
}

func TestDBManagerConcurrentGet(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())

	n := 100
	for i := 0; i < n; i++ {
		manager.Put(fmt.Sprintf("db%d", i), DefaultOptions(), false)
	}

	var wg sync.WaitGroup
	wg.Add(n)

	for idx := 0; idx < n; idx++ {
		go func(idx int) {
			defer wg.Done()

			db, err := manager.Get(idx)
			require.NoError(t, err)
			require.NotNil(t, db)
			defer manager.Release(idx)

			require.LessOrEqual(t, manager.dbCache.EntriesCount(), 5)

			sleepTime := time.Duration(10+rand.Intn(41)) * time.Millisecond
			time.Sleep(sleepTime)
		}(idx)
	}
	wg.Wait()
}

func TestDBManagerOpen(t *testing.T) {
	var nCalls uint64

	openDB := func(name string, opts *Options) (DB, error) {
		atomic.AddUint64(&nCalls, 1)
		return openMockDB(name, opts)
	}

	manager := NewDBManager(openDB, 1, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	n := 1000

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			_, err := manager.Get(0)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	require.Equal(t, nCalls, uint64(1))
	v, err := manager.dbCache.Get(0)
	require.NoError(t, err)

	ref, _ := v.(*dbRef)
	require.NotNil(t, ref)
	require.NotNil(t, ref.db)
	require.Equal(t, ref.count, uint32(n))

	for i := 0; i < n; i++ {
		manager.Release(0)
	}
	require.Zero(t, ref.count)
}

func TestDBManagerClose(t *testing.T) {
	maxActiveDBs := 10
	manager := NewDBManager(openMockDB, maxActiveDBs, logger.NewMemoryLogger())

	manager.Put("test", DefaultOptions(), false)

	n := 100
	for i := 0; i < n; i++ {
		_, err := manager.Get(0)
		require.NoError(t, err)
	}

	err := manager.Close(0)
	require.NoError(t, err)

	err = manager.Close(0)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)

	for i := 0; i < n; i++ {
		manager.Release(0)
	}

	_, err = manager.Get(0)
	require.ErrorIs(t, err, store.ErrAlreadyClosed)
}

func TestDBManagerCloseDuringGet(t *testing.T) {
	maxActiveDBs := 10
	manager := NewDBManager(openMockDB, maxActiveDBs, logger.NewMemoryLogger())

	for i := 0; i <= maxActiveDBs; i++ {
		manager.Put(fmt.Sprintf("test%d", i), DefaultOptions(), false)
	}

	for i := 0; i < maxActiveDBs; i++ {
		_, err := manager.Get(i)
		require.NoError(t, err)
	}

	n := 100

	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()

			_, err := manager.Get(maxActiveDBs)
			require.ErrorIs(t, err, store.ErrAlreadyClosed)
		}()
	}

	// wait for all goroutines to attempt Get(maxActiveDBs)
	time.Sleep(time.Millisecond * 100)

	err := manager.Close(maxActiveDBs)
	require.NoError(t, err)

	wg.Wait()
}

func TestDBManagerDelete(t *testing.T) {
	manager := NewDBManager(openMockDB, 1, logger.NewMemoryLogger())

	manager.Put("test", DefaultOptions(), false)

	err := manager.Delete("test")
	require.ErrorIs(t, err, ErrCannotDeleteAnOpenDatabase)

	err = manager.Close(0)
	require.NoError(t, err)

	err = manager.Delete("test")
	require.NoError(t, err)
}

func TestDBManagerCloseAll(t *testing.T) {
	maxActiveDBs := 10
	manager := NewDBManager(openMockDB, maxActiveDBs, logger.NewMemoryLogger())

	n := 100
	for i := 0; i < n; i++ {
		manager.Put(fmt.Sprintf("test%d", i), DefaultOptions(), false)
	}

	var wg sync.WaitGroup
	wg.Add(maxActiveDBs)
	for i := 0; i < maxActiveDBs; i++ {
		go func(idx int) {
			defer wg.Done()

			_, err := manager.Get(idx)
			require.NoError(t, err)
		}(i)
	}
	wg.Wait()

	var wg1 sync.WaitGroup
	wg1.Add(n - maxActiveDBs)
	for i := maxActiveDBs; i < n; i++ {
		go func(idx int) {
			defer wg1.Done()

			_, err := manager.Get(idx)
			require.ErrorIs(t, err, store.ErrAlreadyClosed)
		}(i)
	}

	t.Run("close deadline exceeded", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := manager.CloseAll(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// Goroutines waiting to acquire a database
		// should be awakened by CloseAll()
		wg1.Wait()
	})

	for i := 0; i < n; i++ {
		manager.Release(i)
	}

	t.Run("close succeeds", func(t *testing.T) {
		err := manager.CloseAll(context.Background())
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			_, err := manager.Get(i)
			require.ErrorIs(t, err, store.ErrAlreadyClosed)
		}
	})
}

func TestLazyDB(t *testing.T) {
	dir := t.TempDir()

	err := os.MkdirAll(filepath.Join(dir, "testdb"), os.ModePerm)
	require.NoError(t, err)

	err = os.MkdirAll(filepath.Join(dir, "testdb1"), os.ModePerm)
	require.NoError(t, err)

	logger := logger.NewMemoryLogger()

	m := NewDBManager(func(name string, opts *Options) (DB, error) {
		return OpenDB(name, nil, opts, logger)
	}, 1, logger)

	dbList := NewDatabaseList(m)
	_, err = dbList.GetByIndex(0)
	require.ErrorIs(t, err, ErrDatabaseNotExists)

	db := dbList.Put("testdb", DefaultOptions().WithDBRootPath(dir))
	db1 := dbList.Put("testdb1", DefaultOptions().WithDBRootPath(dir))
	closedDB := dbList.PutClosed("closeddb", DefaultOptions().WithDBRootPath(dir))

	require.True(t, m.Has("testdb"))
	require.True(t, m.Has("testdb1"))
	require.False(t, db.IsClosed())
	require.False(t, db1.IsClosed())
	require.True(t, closedDB.IsClosed())

	t.Run("isActive", func(t *testing.T) {
		require.False(t, m.IsActive(0))
		require.False(t, db.IsReplica())
		require.True(t, m.IsActive(0))
		require.False(t, db1.IsReplica())
		require.False(t, m.IsActive(0))
		require.True(t, m.IsActive(1))
	})

	t.Run("isReplica", func(t *testing.T) {
		require.False(t, db.IsReplica())
		db.AsReplica(true, false, 0)
		require.True(t, db.IsReplica())

		require.False(t, db1.IsReplica()) // force db1 loading
		require.True(t, db.IsReplica())
	})

	t.Run("SetSyncReplication", func(t *testing.T) {
		db.SetSyncReplication(true)
		require.True(t, db.IsSyncReplicationEnabled())
		require.False(t, db1.IsReplica()) // force db1 loading
		require.True(t, db.IsSyncReplicationEnabled())
	})

	t.Run("CurrentState", func(t *testing.T) {
		state, err := db1.CurrentState()
		require.NoError(t, err)
		require.NotNil(t, state, err)

		s, err := db1.Size()
		require.NoError(t, err)
		require.NotZero(t, s)

		_, err = db1.Set(context.Background(), &schema.SetRequest{
			KVs: []*schema.KeyValue{
				{
					Key: []byte("k1"), Value: []byte("v1"),
				},
			},
		})
		require.NoError(t, err)

		err = db1.WaitForTx(context.Background(), 1, true)
		require.NoError(t, err)

		err = db1.WaitForIndexingUpto(context.Background(), 1)
		require.NoError(t, err)

		s1, err := db1.Size()
		require.NoError(t, err)
		require.Greater(t, s1, s)

		state1, err := db1.CurrentState()
		require.NoError(t, err)
		require.NotEqual(t, state, state1)
		require.True(t, db.IsReplica()) // force db loading

		// calling CurrentState() again should not force db reloading
		state2, err := db1.CurrentState()
		require.NoError(t, err)
		require.Equal(t, state1, state2)
		require.False(t, m.IsActive(1))
	})

	t.Run("copy catalog", func(t *testing.T) {
		_, err := db1.CopySQLCatalog(context.Background(), 1)
		require.NoError(t, err)
	})

	t.Run("truncate", func(t *testing.T) {
		err := db1.TruncateUptoTx(context.Background(), 1)
		require.NoError(t, err)
	})

	t.Run("sql", func(t *testing.T) {
		params, err := db.InferParameters(context.Background(), nil, "SELECT * FROM table1")
		require.ErrorIs(t, err, sql.ErrTableDoesNotExist)
		require.Nil(t, params)

		_, err = db.SQLQueryAll(context.Background(), nil, &schema.SQLQueryRequest{Sql: "SELECT * FROM table1"})
		require.ErrorIs(t, err, sql.ErrTableDoesNotExist)
	})

	t.Run("IsLoaded", func(t *testing.T) {
		require.True(t, m.IsLoaded(0))
		err = m.Close(0)
		require.NoError(t, err)
		require.False(t, m.IsLoaded(0))
	})
}

func TestDBManagerGetIndexByName(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())
	manager.Put("alpha", DefaultOptions(), false)
	manager.Put("beta", DefaultOptions(), false)
	manager.Put("gamma", DefaultOptions(), false)

	require.Equal(t, 0, manager.GetIndexByName("alpha"))
	require.Equal(t, 1, manager.GetIndexByName("beta"))
	require.Equal(t, 2, manager.GetIndexByName("gamma"))
	require.Equal(t, -1, manager.GetIndexByName("nonexistent"))
}

func TestDBManagerGetNameByIndex(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())
	manager.Put("db1", DefaultOptions(), false)
	manager.Put("db2", DefaultOptions(), false)

	require.Equal(t, "db1", manager.GetNameByIndex(0))
	require.Equal(t, "db2", manager.GetNameByIndex(1))
	require.Equal(t, "", manager.GetNameByIndex(-1))
	require.Equal(t, "", manager.GetNameByIndex(999))
}

func TestDBManagerGetOptionsByIndex(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	require.NotNil(t, manager.GetOptionsByIndex(0))
	require.Nil(t, manager.GetOptionsByIndex(-1))
	require.Nil(t, manager.GetOptionsByIndex(999))
}

func TestDBManagerLength(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())
	require.Equal(t, 0, manager.Length())

	manager.Put("a", DefaultOptions(), false)
	manager.Put("b", DefaultOptions(), false)
	manager.Put("c", DefaultOptions(), false)
	require.Equal(t, 3, manager.Length())
}

func TestDBManagerHasIndex(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	require.True(t, manager.HasIndex(0))
	require.False(t, manager.HasIndex(-1))
	require.False(t, manager.HasIndex(999))

	// Close then delete - HasIndex should return false for deleted DBs
	db, err := manager.Get(0)
	require.NoError(t, err)
	require.NotNil(t, db)
	manager.Release(0)

	err = manager.Close(0)
	require.NoError(t, err)

	err = manager.Delete("testdb")
	require.NoError(t, err)

	require.False(t, manager.HasIndex(0))
}

func TestDBManagerResize(t *testing.T) {
	manager := NewDBManager(openMockDB, 2, logger.NewMemoryLogger())

	manager.Put("db0", DefaultOptions(), false)
	manager.Put("db1", DefaultOptions(), false)

	db0, err := manager.Get(0)
	require.NoError(t, err)
	require.NotNil(t, db0)
	manager.Release(0)

	db1, err := manager.Get(1)
	require.NoError(t, err)
	require.NotNil(t, db1)
	manager.Release(1)

	// Resize to larger capacity
	manager.Resize(5)

	manager.Put("db2", DefaultOptions(), false)
	manager.Put("db3", DefaultOptions(), false)
	manager.Put("db4", DefaultOptions(), false)

	for i := 0; i < 5; i++ {
		db, err := manager.Get(i)
		require.NoError(t, err)
		require.NotNil(t, db)
		manager.Release(i)
	}
}

func TestDBManagerPutUpdate(t *testing.T) {
	manager := NewDBManager(openMockDB, 5, logger.NewMemoryLogger())

	idx1 := manager.Put("mydb", DefaultOptions(), false)
	idx2 := manager.Put("mydb", DefaultOptions(), false)

	require.Equal(t, idx1, idx2, "re-putting same name should return same index")
	require.Equal(t, 1, manager.Length(), "should not create duplicate entry")
}

func TestDBManagerCloseAllAfterFailedOpen(t *testing.T) {
	// allocDB caches a placeholder ref before openDB runs. When the open fails,
	// Release must drop that ref: a cached dbRef with a nil db used to poison
	// GetState and IsActive until restart. CloseAll keeps its nil guard as
	// defence in depth, since the ref stays briefly observable in between.
	openErr := fmt.Errorf("open failed")
	openDB := func(name string, opts *Options) (DB, error) {
		return nil, openErr
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	_, err := manager.Get(0)
	require.ErrorIs(t, err, openErr)

	_, err = manager.dbCache.Get(0)
	require.ErrorIs(t, err, cache.ErrKeyNotFound, "failed open must not leave a db-less ref cached")

	require.False(t, manager.IsActive(0), "a database that never opened is not active")

	require.NotPanics(t, func() {
		require.NoError(t, manager.CloseAll(context.Background()))
	})
}

// TestDBManagerFailedOpenDoesNotDeadlock guards the lock-order inversion that
// used to wedge the whole manager: allocDB returns holding db.mtx while having
// taken m.mtx first, and the failure path then wanted m.mtx again via Release.
// A concurrent allocDB for the same index closed the cycle. This hangs if Get
// reacquires m.mtx before releasing db.mtx.
func TestDBManagerFailedOpenDoesNotDeadlock(t *testing.T) {
	openErr := fmt.Errorf("open failed")
	openDB := func(name string, opts *Options) (DB, error) {
		return nil, openErr
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	const n = 50

	done := make(chan struct{})
	go func() {
		defer close(done)

		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				_, err := manager.Get(0)
				require.ErrorIs(t, err, openErr)
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent failed opens deadlocked")
	}

	_, err := manager.dbCache.Get(0)
	require.ErrorIs(t, err, cache.ErrKeyNotFound)
}

// TestDBManagerRetriesAfterFailedOpen is the acceptance criterion for #2155: a
// transient storage error during an open must not be terminal.
func TestDBManagerRetriesAfterFailedOpen(t *testing.T) {
	openErr := fmt.Errorf("transient storage failure")

	var fail atomic.Bool
	fail.Store(true)

	var calls atomic.Uint32
	openDB := func(name string, opts *Options) (DB, error) {
		calls.Add(1)
		if fail.Load() {
			return nil, openErr
		}
		return &mockDB{name: name}, nil
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	_, err := manager.Get(0)
	require.ErrorIs(t, err, openErr)

	// GetState must surface the real open error, not the old "unable to get
	// state" sentinel, and must not serve it forever.
	_, err = manager.GetState(0)
	require.ErrorIs(t, err, openErr)

	info, has := manager.getDB(0)
	require.True(t, has)
	require.NotNil(t, info.openFailure())

	// Age the recorded failure past the retry backoff.
	failure := info.openFailure()
	info.lastOpenFail.Store(&openFailure{err: failure.err, at: failure.at.Add(-2 * openRetryInterval)})

	fail.Store(false)

	// GetState alone re-drives the open: this is what makes the once-a-minute
	// metrics loop a self-healing retry driver.
	state, err := manager.GetState(0)
	require.NoError(t, err)
	require.NotNil(t, state)

	require.True(t, manager.IsActive(0))
	require.Nil(t, info.openFailure())
	require.Empty(t, manager.OpenFailures())
	require.Greater(t, calls.Load(), uint32(1))

	require.NoError(t, manager.CloseAll(context.Background()))
}

func TestDBManagerConcurrentFailedThenSuccessfulOpen(t *testing.T) {
	openErr := fmt.Errorf("open failed")

	var fail atomic.Bool
	fail.Store(true)

	openDB := func(name string, opts *Options) (DB, error) {
		if fail.Load() {
			return nil, openErr
		}
		return &mockDB{name: name}, nil
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			defer wg.Done()

			if i == 10 {
				fail.Store(false)
			}
			if db, err := manager.Get(0); err == nil {
				require.NotNil(t, db)
				manager.Release(0)
			}
		}(i)
	}
	wg.Wait()

	fail.Store(false)

	db, err := manager.Get(0)
	require.NoError(t, err)
	require.NotNil(t, db)
	manager.Release(0)

	v, err := manager.dbCache.Get(0)
	require.NoError(t, err)
	ref := v.(*dbRef)
	require.NotNil(t, ref.db)
	require.Zero(t, atomic.LoadUint32(&ref.count))
	require.Equal(t, 1, manager.dbCache.EntriesCount())

	require.NoError(t, manager.CloseAll(context.Background()))
}

// TestDBManagerCloseDuringFailedOpen guards the pendingClose hand-off: if Close
// parks the ref while the failing open is still counted, the last Release must
// drain it rather than orphan it.
func TestDBManagerCloseDuringFailedOpen(t *testing.T) {
	openErr := fmt.Errorf("open failed")

	opening := make(chan struct{})
	release := make(chan struct{})
	openDB := func(name string, opts *Options) (DB, error) {
		close(opening)
		<-release
		return nil, openErr
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	getDone := make(chan struct{})
	go func() {
		defer close(getDone)

		_, err := manager.Get(0)
		require.ErrorIs(t, err, openErr)
	}()

	// allocDB has returned and the opener holds db.mtx.
	<-opening

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)

		// Blocks on db.mtx until the failing open lets go of it.
		_ = manager.Close(0)
	}()

	time.Sleep(50 * time.Millisecond)
	close(release)

	for _, c := range []chan struct{}{getDone, closeDone} {
		select {
		case <-c:
		case <-time.After(30 * time.Second):
			t.Fatal("failed open racing Close deadlocked")
		}
	}

	manager.mtx.Lock()
	pending := len(manager.pendingClose)
	manager.mtx.Unlock()
	require.Zero(t, pending, "pendingClose must not be orphaned")

	_, err := manager.dbCache.Get(0)
	require.ErrorIs(t, err, cache.ErrKeyNotFound)
}

func TestDBManagerGetOptionsByIndexAfterFailedOpen(t *testing.T) {
	openErr := fmt.Errorf("open failed")
	openDB := func(name string, opts *Options) (DB, error) {
		return nil, openErr
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("testdb", DefaultOptions(), false)

	_, err := manager.Get(0)
	require.ErrorIs(t, err, openErr)

	// A nil *Options here is what lazyDB.Path() and MaxResultSize() dereference.
	opts := manager.GetOptionsByIndex(0)
	require.NotNil(t, opts)

	db := &lazyDB{m: manager, idx: 0}
	require.NotPanics(t, func() { _ = db.Path() })
}

func TestDBManagerOpenFailures(t *testing.T) {
	openErr := fmt.Errorf("open failed")

	var fail atomic.Bool
	fail.Store(true)

	openDB := func(name string, opts *Options) (DB, error) {
		if fail.Load() {
			return nil, openErr
		}
		return &mockDB{name: name}, nil
	}

	manager := NewDBManager(openDB, 5, logger.NewMemoryLogger())
	manager.Put("okdb", DefaultOptions(), false)
	manager.Put("faileddb", DefaultOptions(), false)
	manager.Put("untoucheddb", DefaultOptions(), false)

	require.Empty(t, manager.OpenFailures(), "a database that was never accessed is not a failure")

	_, err := manager.Get(1)
	require.ErrorIs(t, err, openErr)

	failures := manager.OpenFailures()
	require.Len(t, failures, 1)
	require.Equal(t, "faileddb", failures[0].Name)
	require.ErrorIs(t, failures[0].Err, openErr)
	require.False(t, failures[0].At.IsZero())

	fail.Store(false)

	_, err = manager.Get(0)
	require.NoError(t, err)
	manager.Release(0)

	require.Len(t, manager.OpenFailures(), 1, "an unrelated successful open must not clear it")

	_, err = manager.Get(1)
	require.NoError(t, err)
	manager.Release(1)

	require.Empty(t, manager.OpenFailures(), "a successful open clears the recorded failure")

	require.NoError(t, manager.CloseAll(context.Background()))
}
