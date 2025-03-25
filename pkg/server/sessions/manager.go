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

package sessions

import (
	"context"
	"encoding/base64"
	"math"
	"os"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
)

const infinity = time.Duration(math.MaxInt64)

type manager struct {
	running    bool
	sessionMux sync.RWMutex
	sessions   map[string]*Session
	ticker     *time.Ticker
	done       chan bool
	logger     logger.Logger
	options    Options
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
	CommitTransaction(ctx context.Context, transaction transactions.Transaction) ([]*sql.SQLTx, error)
	RollbackTransaction(transaction transactions.Transaction) error
}

func NewManager(options *Options) (*manager, error) {
	if options == nil {
		return nil, ErrInvalidOptionsProvided
	}

	err := options.Validate()
	if err != nil {
		return nil, err
	}

	guard := &manager{
		sessions: make(map[string]*Session),
		ticker:   time.NewTicker(options.SessionGuardCheckInterval),
		done:     make(chan bool),
		logger:   logger.NewSimpleLogger("immudb session guard", os.Stdout),
		options:  *options,
	}

	guard.options.Normalize()

	return guard, nil
}

func (sm *manager) NewSession(user *auth.User, db database.DB) (*Session, error) {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	if len(sm.sessions) >= sm.options.MaxSessions {
		sm.logger.Warningf("max sessions reached")
		return nil, ErrMaxSessionsReached
	}

	randomBytes := make([]byte, 32)
	n, err := sm.options.RandSource.Read(randomBytes)
	if err != nil {
		sm.logger.Errorf("cant create session id: %v", err)
		return nil, ErrCantCreateSessionID
	}
	if n < len(randomBytes) {
		sm.logger.Errorf("cant create session id: could produce enough random data")
		return nil, ErrCantCreateSessionID
	}

	sessionID := base64.URLEncoding.EncodeToString(randomBytes)
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

	session, ok := sm.sessions[sessionID]
	if !ok {
		return nil, ErrSessionNotFound
	}

	return session, nil
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

	merr := multierr.NewMultiErr()

	if err := sess.CloseDocumentReaders(); err != nil {
		merr.Append(err)
	}

	if err := sess.RollbackTransactions(); err != nil {
		merr.Append(err)
	}

	delete(sm.sessions, sessionID)

	return merr.Reduce()
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
	defer sm.sessionMux.Unlock()

	if sm.running {
		return ErrGuardAlreadyRunning
	}
	sm.running = true

	go func() {
		for {
			select {
			case <-sm.done:
				return
			case <-sm.ticker.C:
				sm.expireSessions(time.Now())
			}
		}
	}()

	return nil
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
	sm.ticker.Stop()

	// Wait for the guard to finish any pending cancellation work
	// this must be done with unlocked mutex since
	// mutex expiration may try to lock the mutex
	sm.sessionMux.Unlock()
	sm.done <- true
	sm.sessionMux.Lock()

	// Delete all
	for id := range sm.sessions {
		sm.deleteSession(id)
	}

	sm.logger.Debugf("shutdown")

	return nil
}

func (sm *manager) expireSessions(now time.Time) (sessionsCount, inactiveSessCount, deletedSessCount int, err error) {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()

	if !sm.running {
		return 0, 0, 0, ErrGuardNotRunning
	}

	inactiveSessCount = 0
	deletedSessCount = 0
	sm.logger.Debugf("checking at %s", now.Format(time.UnixDate))
	for ID, sess := range sm.sessions {

		createdAt := sess.GetCreationTime()
		lastActivity := sess.GetLastActivityTime()

		if now.Sub(createdAt) > sm.options.MaxSessionAgeTime {
			sm.logger.Debugf("removing session %s - exceeded MaxSessionAgeTime", ID)
			sm.deleteSession(ID)
			deletedSessCount++
		} else if now.Sub(lastActivity) > sm.options.Timeout {
			sm.logger.Debugf("removing session %s - exceeded Timeout", ID)
			sm.deleteSession(ID)
			deletedSessCount++
		} else if now.Sub(lastActivity) > sm.options.MaxSessionInactivityTime {
			inactiveSessCount++
		}
	}

	sm.logger.Debugf("Open sessions count: %d\n", len(sm.sessions))
	sm.logger.Debugf("Inactive sessions count: %d\n", inactiveSessCount)
	sm.logger.Debugf("Deleted sessions count: %d\n", deletedSessCount)

	return len(sm.sessions), inactiveSessCount, deletedSessCount, nil
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

func (sm *manager) CommitTransaction(ctx context.Context, tx transactions.Transaction) ([]*sql.SQLTx, error) {
	err := sm.DeleteTransaction(tx)
	if err != nil {
		return nil, err
	}
	cTxs, err := tx.Commit(ctx)
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
