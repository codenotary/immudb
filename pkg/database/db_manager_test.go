/*
Copyright 2024 Codenotary Inc. All rights reserved.

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
