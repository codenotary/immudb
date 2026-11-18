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

	mtx          sync.Mutex
	waitCond     *sync.Cond
	pendingClose map[int]*dbRef // refs removed from cache but still in use

	closed bool
}

// openRetryInterval bounds how often GetState re-drives a failed open. Without
// it, every CurrentState call on a permanently broken database would hammer the
// underlying (possibly remote) storage.
const openRetryInterval = 5 * time.Second

// openFailure records a failed attempt to open a database.
type openFailure struct {
	err error
	at  time.Time
}

// DatabaseOpenFailure reports a database whose most recent open attempt failed
// and which has not been opened successfully since.
type DatabaseOpenFailure struct {
	Name string
	Err  error
	At   time.Time
}

type dbInfo struct {
	mtx sync.Mutex

	opts  *Options
	state *schema.ImmutableState

	name    string
	deleted bool
	closed  bool

	// lastOpenFail is read by readiness probes while an open is in flight, so it
	// is accessed atomically rather than under mtx: mtx is held for the entire
	// duration of an open, which is exactly when a probe wants an answer.
	lastOpenFail atomic.Pointer[openFailure]
}

func (db *dbInfo) recordOpenFailure(err error) {
	db.lastOpenFail.Store(&openFailure{err: err, at: time.Now()})
}

func (db *dbInfo) clearOpenFailure() {
	db.lastOpenFail.Store(nil)
}

func (db *dbInfo) openFailure() *openFailure {
	return db.lastOpenFail.Load()
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
	db.clearOpenFailure()

	return nil
}

type dbRef struct {
	db    DB
	count uint32

	// opened is set to 1 once db has been assigned. It lets readers determine
	// whether the ref backs a real database without reading db itself, which is
	// only safe under the owning dbInfo's mutex.
	opened uint32
}

type OpenDBFunc func(name string, opts *Options) (DB, error)

func NewDBManager(openFunc OpenDBFunc, maxActiveDatabases int, log logger.Logger) *DBManager {
	m := &DBManager{
		openDB:       openFunc,
		dbIndex:      make(map[string]int),
		databases:    make([]*dbInfo, 0),
		logger:       log,
		pendingClose: make(map[int]*dbRef),
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
			// Benign: a ref whose open failed can be evicted by another database's
			// allocDB before Release pops it.
			m.logger.Debugf("db not initialised during eviction")
			return
		}

		state, err := ref.db.CurrentState()
		if err != nil {
			m.logger.Errorf(`%v: while fetching db %s state`, err, ref.db.GetName())
		}

		opts := ref.db.GetOptions()

		err = ref.db.Close()
		if err != nil {
			m.logger.Errorf(`%v: while closing db "%s"`, err, ref.db.GetName())
		}

		if i, ok := idx.(int); ok && (i >= 0 && i < len(m.databases)) {
			m.databases[i].cacheInfo(state, opts)
		}
		ref.db = nil
		atomic.StoreUint32(&ref.opened, 0)
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
		ref.clearOpenFailure()
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

	// NOTE: allocDB returns with db.mtx held. It is unlocked explicitly rather
	// than deferred because the failed-open path must release it *before*
	// Release acquires m.mtx: allocDB takes m.mtx then db.mtx, so holding
	// db.mtx while waiting on m.mtx deadlocks against a concurrent allocDB.
	if ref.db == nil {
		d, err := m.openDB(db.name, db.opts)
		if err != nil {
			db.recordOpenFailure(err)
			db.mtx.Unlock()

			// Drops the db-less ref from the cache so a later access retries the
			// open instead of observing a permanently poisoned entry.
			m.Release(idx)
			return nil, err
		}
		ref.db = d
		atomic.StoreUint32(&ref.opened, 1)
		db.clearOpenFailure()
	}

	opened := ref.db
	db.mtx.Unlock()

	return opened, nil
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
	var ref *dbRef

	if v, err := m.dbCache.Get(idx); err == nil {
		ref, _ = v.(*dbRef)
	} else {
		// DB was removed from the active cache (e.g. by Close or eviction).
		// Check whether it is parked in pendingClose.
		m.mtx.Lock()
		ref = m.pendingClose[idx]
		m.mtx.Unlock()
	}

	if ref == nil {
		return
	}

	if atomic.AddUint32(&ref.count, ^uint32(0)) != 0 {
		return
	}

	var toClose DB

	m.mtx.Lock()
	if pc := m.pendingClose[idx]; pc == ref {
		// Close() deferred the underlying close to us; finalize it below.
		delete(m.pendingClose, idx)
		toClose = ref.db
	} else if v, err := m.dbCache.Get(idx); err == nil &&
		v.(*dbRef) == ref &&
		atomic.LoadUint32(&ref.count) == 0 &&
		atomic.LoadUint32(&ref.opened) == 0 {
		// The open failed and nobody picked this ref up in the meantime. Drop it,
		// so a dbRef with a nil db is never observable: leaving it cached is what
		// made a transient storage error terminal until restart.
		m.dbCache.Pop(idx)
	}
	m.waitCond.Signal()
	m.mtx.Unlock()

	if toClose != nil {
		toClose.Close()
	}
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
	return dbInfo.getOptions()
}

func (m *DBManager) GetState(idx int) (*schema.ImmutableState, error) {
	dbInfo, has := m.getDB(idx)
	if !has {
		return nil, ErrDatabaseNotExists
	}

	if ref, err := m.dbCache.Get(idx); err == nil {
		dbInfo.mtx.Lock()
		if dbRef := ref.(*dbRef); dbRef != nil && dbRef.db != nil {
			defer dbInfo.mtx.Unlock()
			return dbRef.db.CurrentState()
		}
		// The open behind this ref failed, or is still in flight. Fall through and
		// let m.Get drive it. The unlock is explicit: m.Get -> allocDB acquires
		// this same mutex, so deferring it here would self-deadlock.
		dbInfo.mtx.Unlock()
	}

	if failure := dbInfo.openFailure(); failure != nil {
		// A recorded failure outranks any state cached by a previous successful
		// open, otherwise a database that fails to *re*open never retries.
		if time.Since(failure.at) < openRetryInterval {
			return nil, failure.err
		}
	} else if s := dbInfo.getState(); s != nil {
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

	// Mark db.closed so allocDB rejects new callers from this point on.
	if err := db.close(); err != nil {
		return err
	}
	defer m.waitCond.Broadcast()

	// Remove from the active cache so that any eviction logic cannot race
	// with our cleanup path.
	m.mtx.Lock()
	v, err := m.dbCache.Pop(idx)
	if err != nil {
		m.mtx.Unlock()
		return nil // not in cache — never opened or already evicted
	}
	ref, _ := v.(*dbRef)
	if ref == nil || atomic.LoadUint32(&ref.count) == 0 {
		m.mtx.Unlock()
		if ref != nil && ref.db != nil {
			ref.db.Close()
		}
		return nil
	}

	// There are still active callers holding this ref.  Park it in
	// pendingClose so that the last Release() will perform the actual close.
	m.pendingClose[idx] = ref
	m.mtx.Unlock()
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
			ref, _ := value.(*dbRef)
			if ref == nil {
				return nil
			}

			if atomic.LoadUint32(&ref.count) > 0 {
				busyDBs++
				return nil
			}

			// Defensive: Release pops the ref left behind by a failed open, but
			// that ref stays briefly observable in between. Guard as every other
			// close site in this file does.
			if ref.db != nil {
				ref.db.Close()
			}
			return nil
		})
		tryClose = busyDBs > 0

		time.Sleep(time.Millisecond * 10)
	}
	m.dbCache.Resize(0)
	return nil
}

// IsActive reports whether the database is currently open. A database whose
// open failed is not active, even while its ref is still cached.
func (m *DBManager) IsActive(idx int) bool {
	v, err := m.dbCache.Get(idx)
	if err != nil {
		return false
	}

	ref, _ := v.(*dbRef)
	return ref != nil && atomic.LoadUint32(&ref.opened) == 1
}

// OpenFailures returns every database whose most recent open attempt failed and
// which has not been opened successfully since. It is the basis of the
// readiness signal: a database that was never accessed has never attempted an
// open and is not reported here.
func (m *DBManager) OpenFailures() []DatabaseOpenFailure {
	m.dbMutex.RLock()
	dbs := make([]*dbInfo, len(m.databases))
	copy(dbs, m.databases)
	m.dbMutex.RUnlock()

	// NOTE: no dbInfo mutex is taken here. db.mtx is held for the whole duration
	// of an open, which is precisely when a probe needs an answer. A closed or
	// deleted database cannot carry a recorded failure: close() and Put() both
	// clear it, and allocDB refuses to open a deleted database.
	var failures []DatabaseOpenFailure
	for _, db := range dbs {
		if failure := db.openFailure(); failure != nil {
			failures = append(failures, DatabaseOpenFailure{Name: db.name, Err: failure.err, At: failure.at})
		}
	}
	return failures
}
