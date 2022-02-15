/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sessions

import (
	"context"
	"math"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
	"github.com/rs/xid"
)

const MaxSessions = 100

const infinity = time.Duration(math.MaxInt64)

type manager struct {
	running    bool
	sessionMux sync.RWMutex
	sessions   map[string]*Session
	ticker     *time.Ticker
	done       chan bool
	logger     logger.Logger
	options    *Options
}

type Manager interface {
	NewSession(user *auth.User, db database.DB) (*Session, error)
	SessionPresent(sessionID string) bool
	DeleteSession(sessionID string) error
	UpdateSessionActivityTime(sessionID string)
	StartSessionsGuard() error
	StopSessionsGuard() error
	GetSession(sessionID string) (*Session, error)
	SessionCount() int
	GetTransactionFromContext(ctx context.Context) (transactions.Transaction, error)
	GetSessionFromContext(ctx context.Context) (*Session, error)
	DeleteTransaction(transactions.Transaction) error
	CommitTransaction(transaction transactions.Transaction) ([]*sql.SQLTx, error)
	RollbackTransaction(transaction transactions.Transaction) error
}

func NewManager(options *Options) (*manager, error) {
	if options == nil {
		return nil, ErrInvalidOptionsProvided
	}
	if options.MaxSessionAgeTime == 0 {
		options.MaxSessionAgeTime = infinity
	}
	if options.MaxSessionInactivityTime == 0 {
		options.MaxSessionInactivityTime = infinity
	}
	if options.Timeout == 0 {
		options.Timeout = infinity
	}
	guard := &manager{
		sessions: make(map[string]*Session),
		ticker:   time.NewTicker(options.SessionGuardCheckInterval),
		done:     make(chan bool),
		logger:   logger.NewSimpleLogger("immudb session guard", os.Stdout),
		options:  options,
	}
	return guard, nil
}

func (sm *manager) NewSession(user *auth.User, db database.DB) (*Session, error) {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	if len(sm.sessions) == MaxSessions {
		sm.logger.Warningf("max sessions reached")
		return nil, ErrMaxSessionsReached
	}

	sessionID := xid.New().String()
	sm.sessions[sessionID] = NewSession(sessionID, user, db, sm.logger)
	sm.logger.Debugf("created session %s", sessionID)

	return sm.sessions[sessionID], nil
}

func (sm *manager) SessionPresent(sessionID string) bool {
	sm.sessionMux.RLock()
	defer sm.sessionMux.RUnlock()

	_, isPresent := sm.sessions[sessionID]
	return isPresent
}

func (sm *manager) GetSession(sessionID string) (*Session, error) {
	sm.sessionMux.RLock()
	defer sm.sessionMux.RUnlock()

	if _, ok := sm.sessions[sessionID]; !ok {
		return nil, ErrSessionNotFound
	}

	return sm.sessions[sessionID], nil
}

func (sm *manager) DeleteSession(sessionID string) error {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	return sm.deleteSession(sessionID)
}

func (sm *manager) deleteSession(sessionID string) error {
	sess, ok := sm.sessions[sessionID]
	if !ok {
		return ErrSessionNotFound
	}

	err := sess.RollbackTransactions()
	delete(sm.sessions, sessionID)
	if err != nil {
		return err
	}

	sess.SetReadWriteTxOngoing(false)

	return nil
}

func (sm *manager) UpdateSessionActivityTime(sessionID string) {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	if sess, ok := sm.sessions[sessionID]; ok {
		now := time.Now()
		sess.SetLastActivityTime(now)
		sm.logger.Debugf("updated last activity time for %s at %s", sessionID, now.Format(time.UnixDate))
	}
}

func (sm *manager) SessionCount() int {
	sm.sessionMux.RLock()
	defer sm.sessionMux.RUnlock()

	return len(sm.sessions)
}

func (sm *manager) StartSessionsGuard() error {
	sm.sessionMux.Lock()
	if sm.running {
		sm.sessionMux.Unlock()
		return ErrGuardAlreadyRunning
	}
	sm.running = true
	sm.sessionMux.Unlock()

	for {
		select {
		case <-sm.done:
			return nil
		case <-sm.ticker.C:
			sm.expireSessions()
		}
	}
}

func (sm *manager) IsRunning() bool {
	sm.sessionMux.RLock()
	defer sm.sessionMux.RUnlock()

	return sm.running
}

func (sm *manager) StopSessionsGuard() error {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	if !sm.running {
		return ErrGuardNotRunning
	}

	sm.running = false

	for ID, _ := range sm.sessions {
		sm.deleteSession(ID)
	}

	sm.ticker.Stop()
	sm.done <- true
	sm.logger.Debugf("shutdown")

	return nil
}

func (sm *manager) expireSessions() {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	if !sm.running {
		return
	}

	now := time.Now()

	inactiveSessCount := 0
	sm.logger.Debugf("checking at %s", now.Format(time.UnixDate))
	for ID, sess := range sm.sessions {
		if sess.GetLastActivityTime().Add(sm.options.MaxSessionInactivityTime).Before(now) && sess.GetStatus() != inactive {
			sess.setStatus(inactive)
			sm.logger.Debugf("session %s became Inactive due to max inactivity time", ID)
		}
		if sess.GetCreationTime().Add(sm.options.MaxSessionAgeTime).Before(now) {
			sess.setStatus(dead)
			sm.logger.Debugf("session %s exceeded MaxSessionAgeTime and became dead", ID)
		}
		if sess.GetStatus() == inactive {
			if sess.GetLastActivityTime().Add(sm.options.Timeout).Before(now) {
				sess.setStatus(dead)
				sm.logger.Debugf("Inactive session %s is dead", ID)
			} else {
				inactiveSessCount++
			}
		}
		if sess.GetStatus() == dead {
			sm.deleteSession(ID)
			sm.logger.Debugf("removed dead session %s", ID)
		}
		sm.logger.Debugf("Open sessions count: %d\nInactive sessions count: %d\n", len(sm.sessions), inactiveSessCount)
	}
}

func (sm *manager) GetTransactionFromContext(ctx context.Context) (transactions.Transaction, error) {
	sessionID, err := GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	sess, err := sm.GetSession(sessionID)
	if err != nil {
		return nil, err
	}

	transactionID, err := GetTransactionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return sess.GetTransaction(transactionID)
}

func (sm *manager) GetSessionFromContext(ctx context.Context) (*Session, error) {
	sessionID, err := GetSessionIDFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return sm.GetSession(sessionID)
}

func (sm *manager) DeleteTransaction(tx transactions.Transaction) error {
	sessionID := tx.GetSessionID()
	sess, err := sm.GetSession(sessionID)
	if err != nil {
		return err
	}
	return sess.RemoveTransaction(tx.GetID())
}

func (sm *manager) CommitTransaction(tx transactions.Transaction) ([]*sql.SQLTx, error) {
	err := sm.DeleteTransaction(tx)
	if err != nil {
		return nil, err
	}
	cTxs, err := tx.Commit()
	if err != nil {
		return nil, err
	}
	return cTxs, nil
}

func (sm *manager) RollbackTransaction(tx transactions.Transaction) error {
	err := sm.DeleteTransaction(tx)
	if err != nil {
		return err
	}
	return tx.Rollback()
}
