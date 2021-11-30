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
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server/sessions/internal/transactions"
	"github.com/rs/xid"
	"os"
	"sync"
	"time"
)

var ErrGuardAlreadyRunning = errors.New("session guard already launched")
var ErrGuardNotRunning = errors.New("session guard not running")

var guard *manager

type manager struct {
	Running    bool
	sessionMux sync.Mutex
	guardMux   sync.Mutex
	sessions   map[string]*Session
	ticker     *time.Ticker
	done       chan bool
	logger     logger.Logger
	options    *Options
}

type Manager interface {
	NewSession(user *auth.User, databaseID int64) string
	SessionPresent(sessionID string) bool
	DeleteSession(sessionID string) error
	UpdateSessionActivityTime(sessionID string)
	UpdateHeartBeatTime(sessionID string)
	StartSessionsGuard() error
	StopSessionsGuard() error
	GetSession(sessionID string) (*Session, error)
	CountSession() int
	GetTransactionFromContext(ctx context.Context) (transactions.Transaction, error)
	DeleteTransaction(transactions.Transaction) error
}

func NewManager(options *Options) *manager {
	if options == nil {
		options = DefaultOptions()
	}
	guard = &manager{
		sessionMux: sync.Mutex{},
		guardMux:   sync.Mutex{},
		sessions:   make(map[string]*Session),
		ticker:     time.NewTicker(options.SessionGuardCheckInterval),
		done:       make(chan bool),
		logger:     logger.NewSimpleLogger("immudb session guard", os.Stdout),
		options:    options,
	}
	return guard
}

func (sm *manager) NewSession(user *auth.User, databaseID int64) string {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()
	sessionID := xid.New().String()
	sm.sessions[sessionID] = NewSession(sessionID, user, databaseID, sm.logger)
	sm.logger.Debugf("created session %s", sessionID)
	return sessionID
}

func (sm *manager) SessionPresent(sessionID string) bool {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()
	if _, ok := sm.sessions[sessionID]; ok {
		return true
	}
	return false
}

func (sm *manager) GetSession(sessionID string) (*Session, error) {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()
	if _, ok := sm.sessions[sessionID]; !ok {
		return nil, ErrSessionNotFound
	}
	return sm.sessions[sessionID], nil
}

func (sm *manager) DeleteSession(sessionID string) error {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()
	sess, ok := sm.sessions[sessionID]
	if !ok {
		return ErrSessionNotFound
	}
	sess.RollbackTransactions()

	delete(sm.sessions, sessionID)
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

func (sm *manager) UpdateHeartBeatTime(sessionID string) {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()
	if sess, ok := sm.sessions[sessionID]; ok {
		now := time.Now()
		sess.SetLastHeartBeat(now)
		sm.logger.Debugf("updated last heart beat time for %s at %s", sessionID, now.Format(time.UnixDate))
	}
}

func (sm *manager) CountSession() int {
	sm.sessionMux.Lock()
	defer sm.sessionMux.Unlock()
	return len(sm.sessions)
}

func (sm *manager) StartSessionsGuard() error {
	sm.guardMux.Lock()
	if sm.Running == true {
		return ErrGuardAlreadyRunning
	}
	sm.Running = true
	sm.guardMux.Unlock()
	for {
		select {
		case <-sm.done:
			return nil
		case <-sm.ticker.C:
			sm.expireSessions()
		}
	}
}

func (sm *manager) StopSessionsGuard() error {
	sm.guardMux.Lock()
	defer sm.guardMux.Unlock()
	sm.sessionMux.Lock()
	if sm.Running == false {
		return ErrGuardNotRunning
	}
	sm.Running = false
	sm.sessionMux.Unlock()
	for ID, _ := range sm.sessions {
		sm.DeleteSession(ID)
	}
	sm.ticker.Stop()
	sm.done <- true
	sm.logger.Debugf("shutdown")
	return nil
}

func (sm *manager) expireSessions() {
	sm.sessionMux.Lock()
	if !sm.Running {
		return
	}
	sm.sessionMux.Unlock()

	now := time.Now()
	sm.logger.Debugf("checking at %s", now.Format(time.UnixDate))
	for ID, sess := range sm.sessions {
		if sess.GetLastHeartBeat().Add(sm.options.MaxSessionIdle).Before(now) && sess.GetStatus() != IDLE {
			sess.SetStatus(IDLE)
			sm.logger.Debugf("session %s became IDLE, no more heartbeat received", ID)
		}
		if sess.GetLastActivityTime().Add(sm.options.MaxSessionIdle).Before(now) && sess.GetStatus() != IDLE {
			sess.SetStatus(IDLE)
			sm.logger.Debugf("session %s became IDLE due to max inactivity time", ID)
		}
		if sess.GetCreationTime().Add(sm.options.MaxSessionAge).Before(now) {
			sess.SetStatus(DEAD)
			sm.logger.Debugf("session %s exceeded MaxSessionAge and became DEAD", ID)
		}
		if sess.GetStatus() == IDLE {
			if sess.GetLastActivityTime().Add(sm.options.Timeout).Before(now) {
				sess.SetStatus(DEAD)
				sm.logger.Debugf("IDLE session %s is DEAD", ID)
			}
			if sess.GetLastHeartBeat().Add(sm.options.Timeout).Before(now) {
				sess.SetStatus(DEAD)
				sm.logger.Debugf("IDLE session %s is DEAD", ID)
			}
		}
		if sess.GetStatus() == DEAD {
			sm.DeleteSession(ID)
			sm.logger.Debugf("removed DEAD session %s", ID)
		}
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

func (sm *manager) DeleteTransaction(tx transactions.Transaction) error {
	sessionID := tx.GetParentSessionID()
	sess, err := sm.GetSession(sessionID)
	if err != nil {
		return err
	}
	sess.RemoveTransaction(tx.GetID())
	return nil
}
