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

package database

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
)

type DBManager struct {
	openDB  OpenDBFunc
	dbCache *cache.Cache

	logger logger.Logger

	dbMutex   sync.RWMutex
	databases []*dbInfo
	dbIndex   map[string]int

	mtx      sync.Mutex
	waitCond *sync.Cond

	closed bool
}

type dbInfo struct {
	mtx sync.Mutex

	opts  *Options
	state *schema.ImmutableState

	name    string
	deleted bool
	closed  bool
}

func (db *dbInfo) cacheInfo(s *schema.ImmutableState, opts *Options) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	db.state = s
	db.opts = opts
}

func (db *dbInfo) getState() *schema.ImmutableState {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	return db.state
}

func (db *dbInfo) getOptions() *Options {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	return db.opts
}

func (db *dbInfo) close() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if db.closed {
		return store.ErrAlreadyClosed
	}
	db.closed = true

	return nil
}

type dbRef struct {
	db    DB
	count uint32
}

type OpenDBFunc func(name string, opts *Options) (DB, error)

func NewDBManager(openFunc OpenDBFunc, maxActiveDatabases int, log logger.Logger) *DBManager {
	m := &DBManager{
		openDB:    openFunc,
		dbIndex:   make(map[string]int),
		databases: make([]*dbInfo, 0),
		logger:    log,
	}
	m.dbCache = createCache(m, maxActiveDatabases)
	m.waitCond = sync.NewCond(&m.mtx)
	return m
}

func createCache(m *DBManager, capacity int) *cache.Cache {
	c, _ := cache.NewCache(capacity)

	c.SetCanEvict(func(_, value interface{}) bool {
		ref, _ := value.(*dbRef)

		return ref != nil && atomic.LoadUint32(&ref.count) == 0
	})

	c.SetOnEvict(func(idx, value interface{}) {
		ref, _ := value.(*dbRef)
		if ref == nil {
			return
		}

		// NOTE: db cannot be nil at this point,
		// since it can only be evicted after it has been successfully opened.
		// Moreover, since the reference cannot be altered after it has been set,
		// there is not need to acquire the database lock.
		if ref.db == nil {
			m.logger.Errorf("db not initialised during eviction")
			return
		}

		state, err := ref.db.CurrentState()
		if err != nil {
			m.logger.Errorf(`%w: while fetching db %s state`, err, ref.db.GetName())
		}

		opts := ref.db.GetOptions()

		err = ref.db.Close()
		if err != nil {
			m.logger.Errorf(`%w: while closing db "%s"`, err, ref.db.GetName())
		}

		if i, ok := idx.(int); ok && (i >= 0 && i < len(m.databases)) {
			m.databases[i].cacheInfo(state, opts)
		}
		ref.db = nil
	})
	return c
}

func (m *DBManager) Put(dbName string, opts *Options, closed bool) int {
	m.dbMutex.Lock()
	defer m.dbMutex.Unlock()

	if idx, has := m.dbIndex[dbName]; has {
		ref := m.databases[idx]
		ref.deleted = false
		ref.closed = closed
		ref.opts = opts
		return idx
	}

	m.dbIndex[dbName] = len(m.databases)

	info := &dbInfo{
		opts:    opts,
		name:    dbName,
		deleted: false,
		closed:  closed,
	}

	m.databases = append(m.databases, info)
	return len(m.databases) - 1
}

func (m *DBManager) Get(idx int) (DB, error) {
	db, exists := m.getDB(idx)
	if !exists {
		return nil, ErrDatabaseNotExists
	}

	ref, err := m.allocDB(idx, db)
	if err != nil {
		return nil, err
	}
	defer db.mtx.Unlock()

	if ref.db == nil {
		d, err := m.openDB(db.name, db.opts)
		if err != nil {
			m.Release(idx)
			return nil, err
		}
		ref.db = d
	}
	return ref.db, nil
}

func (m *DBManager) allocDB(idx int, db *dbInfo) (*dbRef, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for {
		db.mtx.Lock()

		if m.closed || db.closed || db.deleted {
			db.mtx.Unlock()
			return nil, store.ErrAlreadyClosed
		}

		v, err := m.dbCache.Get(idx)
		if err == nil {
			ref := v.(*dbRef)
			atomic.AddUint32(&ref.count, 1)
			return ref, nil
		}

		ref := &dbRef{count: 1}
		_, _, err = m.dbCache.Put(idx, ref)
		if err == nil {
			return ref, nil
		}

		db.mtx.Unlock()
		m.waitCond.Wait()
	}
}

func (m *DBManager) Release(idx int) {
	v, err := m.dbCache.Get(idx)

	// NOTE: may occur if the database is closed
	// before being fully released
	if err != nil {
		return
	}

	ref, _ := v.(*dbRef)
	if ref == nil {
		return
	}

	if atomic.AddUint32(&ref.count, ^uint32(0)) == 0 {
		m.signal()
	}
}

func (m *DBManager) signal() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.waitCond.Signal()
}

func (m *DBManager) Has(name string) bool {
	m.dbMutex.RLock()
	defer m.dbMutex.RUnlock()

	_, has := m.dbIndex[name]
	return has
}

func (m *DBManager) HasIndex(idx int) bool {
	db, exists := m.getDB(idx)
	if !exists {
		return false
	}

	db.mtx.Lock()
	defer db.mtx.Unlock()

	return !db.deleted
}

func (m *DBManager) GetIndexByName(name string) int {
	m.dbMutex.RLock()
	defer m.dbMutex.RUnlock()

	idx, exists := m.dbIndex[name]
	if !exists {
		return -1
	}
	return idx
}

func (m *DBManager) GetNameByIndex(idx int) string {
	m.dbMutex.RLock()
	defer m.dbMutex.RUnlock()

	if idx < 0 || idx >= len(m.databases) {
		return ""
	}
	return m.databases[idx].name
}

func (m *DBManager) GetOptionsByIndex(idx int) *Options {
	dbInfo, has := m.getDB(idx)
	if !has {
		return nil
	}

	ref, err := m.dbCache.Get(idx)
	if err == nil {
		dbInfo.mtx.Lock()
		defer dbInfo.mtx.Unlock()

		if dbRef := ref.(*dbRef); dbRef != nil && dbRef.db != nil {
			return dbInfo.opts
		}
		return nil
	}
	return dbInfo.getOptions()
}

func (m *DBManager) GetState(idx int) (*schema.ImmutableState, error) {
	dbInfo, has := m.getDB(idx)
	if !has {
		return nil, ErrDatabaseNotExists
	}

	ref, err := m.dbCache.Get(idx)
	if err == nil {
		dbInfo.mtx.Lock()
		defer dbInfo.mtx.Unlock()

		if dbRef := ref.(*dbRef); dbRef != nil && dbRef.db != nil {
			return dbRef.db.CurrentState()
		}
		// this condition should never happen
		return nil, fmt.Errorf("unable to get state")
	}

	s := dbInfo.getState()
	if s != nil {
		return s, nil
	}

	db, err := m.Get(idx)
	if err != nil {
		return nil, err
	}
	defer m.Release(idx)

	return db.CurrentState()
}

func (m *DBManager) Delete(name string) error {
	m.dbMutex.RLock()

	idx, exists := m.dbIndex[name]
	if !exists {
		m.dbMutex.RUnlock()
		return ErrDatabaseNotExists
	}

	db := m.databases[idx]
	m.dbMutex.RUnlock()

	db.mtx.Lock()
	defer db.mtx.Unlock()

	if !db.closed {
		return ErrCannotDeleteAnOpenDatabase
	}
	db.deleted = true

	// NOTE: a closed database cannot be present in the cache
	return nil
}

func (m *DBManager) Length() int {
	m.dbMutex.RLock()
	defer m.dbMutex.RUnlock()

	return len(m.databases)
}

func (m *DBManager) IsLoaded(idx int) bool {
	db, exists := m.getDB(idx)
	if !exists {
		return false
	}

	db.mtx.Lock()
	defer db.mtx.Unlock()

	return !db.closed
}

func (m *DBManager) Close(idx int) error {
	db, exists := m.getDB(idx)
	if !exists {
		return nil
	}

	if err := db.close(); err != nil {
		return err
	}
	defer m.waitCond.Broadcast()

	v, err := m.dbCache.Pop(idx)
	if err != nil {
		return nil
	}

	ref, _ := v.(*dbRef)
	if ref != nil && ref.db != nil {
		ref.db.Close()
		ref.db = nil
	}
	return nil
}

func (m *DBManager) IsClosed(idx int) bool {
	db, exists := m.getDB(idx)
	if !exists {
		return true
	}

	db.mtx.Lock()
	defer db.mtx.Unlock()

	return db.closed
}

func (m *DBManager) getDB(idx int) (*dbInfo, bool) {
	m.dbMutex.RLock()
	defer m.dbMutex.RUnlock()

	if idx < 0 || idx >= len(m.databases) {
		return nil, false
	}
	return m.databases[idx], true
}

func (m *DBManager) Resize(n int) {
	m.dbCache.Resize(n)
}

func (m *DBManager) CloseAll(ctx context.Context) error {
	m.mtx.Lock()
	m.closed = true
	m.mtx.Unlock()

	m.waitCond.Broadcast()

	tryClose := true
	for tryClose {
		if err := ctx.Err(); err != nil {
			return err
		}

		busyDBs := 0
		m.dbCache.Apply(func(_, value interface{}) error {
			ref := value.(*dbRef)

			if atomic.LoadUint32(&ref.count) > 0 {
				busyDBs++
				return nil
			}

			ref.db.Close()
			return nil
		})
		tryClose = busyDBs > 0

		time.Sleep(time.Millisecond * 10)
	}
	m.dbCache.Resize(0)
	return nil
}

func (m *DBManager) IsActive(idx int) bool {
	_, err := m.dbCache.Get(idx)
	return err == nil
}
